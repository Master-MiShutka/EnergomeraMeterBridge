#!/usr/bin/env python3
"""Генератор блоков учёта для розеток с мониторингом.

Идея: не придумывать структуру заново, а брать готовые блоки из
energy_devices.yaml (эталон — бойлер) и подставлять в них данные новой розетки.
Так новые розетки учитываются ровно так же, как уже работающие.

Что создаётся для каждой розетки:
    sensor.<slug>_cost_rate / _saving_rate      BYN/ч
    sensor.<slug>_cost_total / _savings_total   BYN (накопленные)
    sensor.<slug>_cost_month / _cost_today      BYN
    sensor.<slug>_saving_month                  BYN
    sensor.<slug>_energy_month                  кВт·ч
    input_number.<slug>_cost_last_month         снимок прошлого месяца

Розетка определяется так: у устройства есть сенсор с device_class power и
сенсор с device_class energy. Пространство берётся из реестра — оно попадёт
в отчёт и в разбивку по ветвям (Мастерская / Жилой дом и т.д.).

Розетки, уже описанные вручную в energy_devices.yaml, пропускаются: генератор
смотрит, какие сенсоры мощности там используются.

Запуск: python3 /config/tools/energy_plugs.py
Результат: /config/packages/energy/energy_plugs.yaml (после этого template.reload).
"""

import copy
import json
import os
import pathlib
import re
import sys
import time

import yaml

CONFIG = pathlib.Path("/config")
STORAGE = CONFIG / ".storage"
SOURCE = CONFIG / "packages" / "energy" / "energy_devices.yaml"
OUTPUT = CONFIG / "packages" / "energy" / "energy_plugs.yaml"
STATES_FILE = pathlib.Path(os.environ.get("STATES_FILE", "/config/tmp_states.json"))
RESTORE_FILE = CONFIG / ".storage" / "core.restore_state"
REFERENCE_SLUG = "boiler"          # чей блок берём за образец


def load_states() -> dict:
    """Читает состояния. Кнопка на панели запускает генератор без токена API,
    поэтому основным источником служит core.restore_state — в нём лежат
    атрибуты (device_class, единицы), которые нужны для поиска розеток."""
    for path in (STATES_FILE, RESTORE_FILE):
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            continue
        items = raw.get("data") if isinstance(raw, dict) else raw
        if not isinstance(items, list):
            continue
        result = {}
        for item in items:
            if isinstance(item.get("state"), dict):          # формат core.restore_state
                inner = item["state"]
                entity = inner.get("entity_id")
                if entity:
                    result[entity] = {"entity_id": entity, "state": inner.get("state"),
                                      "attributes": inner.get("attributes") or {}}
            elif item.get("entity_id"):
                result[item["entity_id"]] = item
        if result:
            print(f"  состояния взяты из {path} ({len(result)} записей)")
            return result
    raise SystemExit("не найден источник состояний")

HEADER = """# ============================================================================
#  Учёт по розеткам — файл создан генератором tools/energy_plugs.py
#
#  Руками не редактировать: при следующем запуске файл перезаписывается целиком.
#  Ручные описания живут в energy_devices.yaml.
#
#  Чтобы добавить розетку: назначьте ей пространство в Home Assistant и запустите
#  генератор (кнопка «Обновить список розеток» на панели), затем template.reload.
#
#  Создано: {stamp} | розеток: {count}
#  Пространства: {areas}
# ============================================================================

"""


def load_registry(name: str) -> dict:
    return json.loads((STORAGE / name).read_text(encoding="utf-8"))["data"]


def referenced_entities(text: str) -> set[str]:
    return set(re.findall(r"(sensor\.[a-z0-9_]+)", text))


def collect_blocks(data: dict, mentions: set[str], marker: str) -> list:
    """Собирает элементы YAML, в которых упоминается эталон (например, boiler)."""
    result = []
    for key in ("template", "sensor", "utility_meter", "input_number", "binary_sensor"):
        section = data.get(key)
        if not section:
            continue
        if key == "template" and isinstance(section, list):
            for group in section:
                for kind, items in (group or {}).items():
                    for item in items or []:
                        blob = json.dumps(item, ensure_ascii=False)
                        if marker in blob or any(m in blob for m in mentions):
                            result.append((key, kind, item))
            continue
        items = section if isinstance(section, list) else [{"__key__": name, "value": value}
                                                           for name, value in section.items()]
        for item in items:
            blob = json.dumps(item, ensure_ascii=False)
            if marker in blob or any(m in blob for m in mentions):
                result.append((key, None, item))
    return result


def substitute(value, replacements: dict):
    if isinstance(value, str):
        for old, new in replacements.items():
            value = value.replace(old, new)
        return value
    if isinstance(value, list):
        return [substitute(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: substitute(item, replacements) for key, item in value.items()}
    return value


def main() -> int:
    states = load_states()
    entities = load_registry("core.entity_registry")["entities"]
    devices = {item["id"]: item for item in load_registry("core.device_registry")["devices"]}
    areas = {item["id"]: item["name"] for item in load_registry("core.area_registry")["areas"]}

    source_text = SOURCE.read_text(encoding="utf-8")
    source_data = yaml.safe_load(source_text)

    # сенсоры мощности, уже описанные вручную (в шаблонах «ставка в час»)
    manual_power = {match for match in re.findall(r"states\('(sensor\.[a-z0-9_]+)'\)", source_text)
                    if re.search(r"(_power|_moshchnost|electric_power)$", match)}

    # эталон — сенсор мощности, использованный в блоке boiler_cost_rate
    found = re.search(r"boiler_cost_rate[\s\S]{0,900}?states\('(sensor\.[a-z0-9_]+)'\)", source_text)
    reference_power = found.group(1) if found else None
    if not reference_power:
        print("  не найден эталонный блок бойлера — генерация невозможна")
        return 1
    reference_energy = next((item for item in re.findall(r"source:\s*(sensor\.[a-z0-9_]+)", source_text)
                             if "boiler" in item
                             and (states.get(item, {}).get("attributes") or {}).get("device_class") == "energy"), None)
    mentions = {reference_power} | ({reference_energy} if reference_energy else set())
    blocks = collect_blocks(source_data, mentions, REFERENCE_SLUG)
    print(f"  эталон: {REFERENCE_SLUG} | сенсор мощности {reference_power} | блоков скопировано: {len(blocks)}")

    by_device: dict[str, list[str]] = {}
    for item in entities:
        if item.get("device_id") and not item.get("disabled_by"):
            by_device.setdefault(item["device_id"], []).append(item["entity_id"])

    plugs = []
    for device_id, entity_ids in by_device.items():
        power = next((e for e in entity_ids
                      if (states.get(e, {}).get("attributes") or {}).get("device_class") == "power"), None)
        energy = next((e for e in entity_ids
                       if (states.get(e, {}).get("attributes") or {}).get("device_class") == "energy"
                       and "energy" in e), None)
        if not power or not energy or power in manual_power:
            continue
        device = devices[device_id]
        name = device.get("name_by_user") or device.get("name") or device_id
        # счётчики и зарядная станция учитываются отдельно: у них своя логика тарифов
        if re.search(r"счётчик|gran|общий|зарядн|\bev\b|ev-", name, re.I):
            continue
        plugs.append({"power": power, "energy": energy, "name": name,
                      "area": areas.get(device.get("area_id"), "")})

    plugs.sort(key=lambda item: item["name"])
    print(f"  розеток к учёту: {len(plugs)}")
    for plug in plugs:
        print(f"    {plug['name']} | {plug['area'] or 'без пространства'} | {plug['power']}")

    if not plugs:
        OUTPUT.write_text(HEADER.format(stamp=time.strftime("%Y-%m-%d %H:%M"), count=0, areas="—")
                          + "template: []\n", encoding="utf-8", newline="\n")
        print("  нечего генерировать, файл очищен")
        return 0

    result = {"template": [], "sensor": [], "utility_meter": {}, "input_number": {}}
    for plug in plugs:
        replacements = {
            reference_power: plug["power"],
            reference_energy or "": plug["energy"],
            REFERENCE_SLUG: re.sub(r"[^a-z0-9]+", "_", plug["name"].lower()).strip("_") or "plug",
            REFERENCE_SLUG.capitalize(): plug["name"],
        }
        replacements.pop("", None)
        for key, kind, item in blocks:
            new_item = substitute(copy.deepcopy(item), replacements)
            if key == "template":
                result["template"].append({"sensor": [new_item]})
            elif key in ("sensor",):
                result["sensor"].append(new_item)
            elif key == "utility_meter":
                name = new_item.pop("__key__", None)
                if name:
                    result["utility_meter"][substitute(name, replacements)] = new_item
            elif key == "input_number":
                name = new_item.pop("__key__", None)
                if name:
                    result["input_number"][substitute(name, replacements)] = new_item

    clean = {key: value for key, value in result.items() if value}
    OUTPUT.write_text(HEADER.format(stamp=time.strftime("%Y-%m-%d %H:%M"), count=len(plugs),
                                    areas=", ".join(sorted({p["area"] for p in plugs if p["area"]})) or "—")
                      + yaml.safe_dump(clean, allow_unicode=True, sort_keys=False, width=300),
                      encoding="utf-8", newline="\n")
    print(f"  записано: {OUTPUT}")
    print(f"  разделы: { {k: (len(v) if not isinstance(v, dict) else len(v)) for k, v in clean.items()} }")
    return 0


if __name__ == "__main__":
    sys.exit(main())
