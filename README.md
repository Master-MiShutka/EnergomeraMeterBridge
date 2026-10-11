# Energomera Meter Bridge — репозиторий приложений Home Assistant

Здесь публикуется приложение для Home Assistant, которое опрашивает электросчётчик
**Энергомера CE301/CE303** через TCP-шлюз по протоколу IEC 61107 и публикует
показания в MQTT с автоматическим созданием сущностей.

| Приложение | Что даёт | Файлы |
| --- | --- | --- |
| **Energomera Meter Bridge** (`meter_bridge`) | Энергия (сумма и тарифы T1–T5), мощность, напряжение и ток по фазам, месячные архивы, статус связи | [Описание](energomera_meter_bridge/README.md) · [Документация](energomera_meter_bridge/DOCS.md) · [Изменения](energomera_meter_bridge/CHANGELOG.md) |

## Установка

1. **Настройки → Дополнения → Магазин**.
2. **⋮** (в правом верхнем углу) → **Репозитории**.
3. Вставьте адрес репозитория и нажмите **Добавить**:

   ```text
   https://github.com/Master-MiShutka/EnergomeraMeterBridge
   ```

4. Обновите страницу магазина, откройте **Energomera Meter Bridge** → **Установить**.
5. Вкладка **Настройки** — заполните параметры (минимум `meter_host`, `meter_port`,
   `mqtt_host`) → **Сохранить** → **Запустить**.

Добавить репозиторий одной кнопкой:

[![Открыть Home Assistant и добавить репозиторий](https://my.home-assistant.io/badges/supervisor_add_addon_repository.svg)](https://my.home-assistant.io/redirect/supervisor_add_addon_repository/?repository_url=https%3A%2F%2Fgithub.com%2FMaster-MiShutka%2FEnergomeraMeterBridge)

После добавления репозитория обновления приложения приходят вместе с обновлениями
Home Assistant: достаточно поднять версию в
[`config.yaml`](energomera_meter_bridge/config.yaml) и дополнить
[`CHANGELOG.md`](energomera_meter_bridge/CHANGELOG.md).

## Структура репозитория

```text
.
├── repository.yaml                  # описание репозитория для Supervisor
├── LICENSE                          # MIT
├── .gitattributes                   # окончания строк: .sh и Dockerfile — LF
├── .gitignore
├── README.md                        # этот файл
├── .github/workflows/builder.yaml   # сборка и публикация образов в GHCR
├── examples/                        # примеры панели «Электроэнергия» и вспомогательных сущностей
├── _source/                         # исходные картинки в высоком разрешении (не публикуются)
└── energomera_meter_bridge/         # само приложение
    ├── config.yaml                  # опции, схема, архитектуры, image, url
    ├── Dockerfile                   # образ приложения
    ├── run.sh                       # точка входа
    ├── meter_bridge.py              # опрос счётчика и публикация в MQTT
    ├── DOCS.md                      # документация (вкладка «Документация»)
    ├── README.md                    # краткое описание (витрина в магазине)
    ├── CHANGELOG.md                 # история изменений
    ├── LICENSE                      # копия лицензии: папка приложения самодостаточна
    ├── apparmor.txt                 # профиль AppArmor (рейтинг безопасности 5 из 6)
    ├── icon.png                     # иконка 128×128
    ├── logo.png                     # логотип 250×193
    └── translations/                # переводы опций (ru, en)
```

## Образы приложения

Готовые образы собираются GitHub Actions
([`.github/workflows/builder.yaml`](.github/workflows/builder.yaml)) официальными
builder-actions Home Assistant и публикуются в GitHub Container Registry как
multi-arch манифест `ghcr.io/master-mishutka/energomera-meter-bridge` с тегом,
равным версии приложения. Пакет в GHCR должен быть публичным — иначе Supervisor
не сможет скачать образ.

## Поддержка

Вопросы и ошибки — в [Issues](https://github.com/Master-MiShutka/EnergomeraMeterBridge/issues).
Перед обращением приложите журнал приложения с опцией `log_level: debug` и вывод
`python3 meter_bridge.py --probe` (см.
[«Диагностика»](energomera_meter_bridge/DOCS.md#диагностика-и-частые-проблемы)).

## Лицензия

Проект распространяется по лицензии [MIT](LICENSE).

Изображения приложения (`icon.png`, `logo.png`) сгенерированы автором проекта.
Исходники в высоком разрешении лежат в `_source/` и в репозиторий не публикуются.

Copyright (c) 2026 Trus Mikhail (@Master-MiShutka)
