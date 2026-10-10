#!/usr/bin/env python3
"""
Опрашивает счётчик Энергомера CE301 через TCP-шлюз и публикует
значения в MQTT с Home Assistant AutoDiscovery.
"""

import os
import json
import logging
import signal
import socket
import time
import sys

import paho.mqtt.client as mqtt
from paho.mqtt.client import CallbackAPIVersion

from IEC61107.IEC61107 import IEC61107, TCP_transport

import re
from datetime import datetime, timedelta

# Символы протокола IEC 61107
SOH = b'\x01'
STX = b'\x02'
ETX = b'\x03'

# Константа: префикс discovery для Home Assistant MQTT AutoDiscovery.
# Меняется крайне редко, поэтому вынесена в код.
DISCOVERY_PREFIX = "homeassistant"

# Режим чтения массива общего чтения: счётчик сам отдаёт все параметры после
# ACK с переходом в режим чтения (см. open_meter / read_all_from_array).
READ_MODE_ARRAY = "array"

# Сколько значений содержит одна запись параметра в массиве общего чтения.
# Записи идут подряд: у ENMPE 12 месяцев по 6 значений, у EADPE 45 суток по 6,
# у MAXPE 12 месяцев по 5, у ET0PE одна запись из 6 значений.
ARRAY_RECORD_SIZE = {
    "ET0PE": 6,   # энергия: суммарная + тарифы T1…T5
    "ENMPE": 6,   # энергия на конец месяца: 12 месяцев × 6 значений
    "EADPE": 6,   # расход за сутки: 45 суток × 6 значений
    "MAXPE": 5,   # максимумы мощности: 12 месяцев × 5 тарифов
    "VOLTA": 3,   # напряжение фаз A, B, C
    "CURRE": 3,   # ток фаз A, B, C
    "POWEP": 1,   # мгновенная активная мощность
}

# Сколько значений параметра нужно собрать, чтобы опубликовать его целиком.
# Месячные архивы — все 12 записей: месяц выбирается по накопленной энергии,
# поэтому нужен весь набор, иначе выбор будет неверным.
ARRAY_NEEDED_VALUES = {
    "ET0PE": 6,
    "ENMPE": 6 * 12,
    "MAXPE": 5 * 12,
    "POWEP": 1,
    "VOLTA": 3,
    "CURRE": 3,
}

# Коды ошибок счётчика (инструкция по программированию, п. 5.3.3.5).
METER_ERRORS = {
    "E05": "ошибка протокола",
    "E12": "неподдерживаемый параметр",
    "E17": "недопустимое значение аргумента запроса",
    "E18": "отсутствует параметр, определяемый значением аргумента",
    "E22": "размер запрошенной информации превышает размер выходного буфера",
}

# Энергия выдаётся массивом из 6 одноимённых значений: первое — суммарная,
# остальные — накопленная в период действия тарифов 1..5
# (инструкция, таблица А.1, параметр EXYZT).
ENERGY_TARIFF_LABELS = [
    ("total", "total"), ("t1", "T1"), ("t2", "T2"),
    ("t3", "T3"), ("t4", "T4"), ("t5", "T5"),
]

# Максимальная мощность выдаётся по каждому из 5 тарифов, без суммарного
# значения (инструкция, таблица А.1, параметр MAXyz).
MAX_POWER_TARIFF_LABELS = [
    ("t1", "T1"), ("t2", "T2"), ("t3", "T3"), ("t4", "T4"), ("t5", "T5"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("meter_bridge")

running = True

USAGE = """\
Использование: python3 meter_bridge.py [ключи]

  (без ключей)   обычный опрос счётчика и публикация в MQTT
  --probe        диагностика канала до счётчика, без MQTT
  --dump-array   снять массив общего чтения и показать порядок параметров
  -d, --debug    подробный журнал: кадры обмена, байты, тайминги
  -h, --help     эта справка

Уровень журнала также задаётся опцией аддона log_level или переменной
окружения METER_BRIDGE_LOG_LEVEL (debug | info | warning).
"""


# ---------- ДИАГНОСТИКА: ФОРМАТИРОВАНИЕ, КАДРЫ, ЖУРНАЛ ----------


def hexdump(data) -> str:
    """Байты в HEX: '01 52 31 02 ...' — для журнала обмена."""
    return " ".join(f"{b:02X}" for b in bytes(data))


def printable(data) -> str:
    """Байты как ASCII, непечатаемые — точками."""
    return "".join(chr(b) if 32 <= b < 127 else "." for b in bytes(data))


def script_dir_path(name: str) -> str:
    """
    Путь к файлу рядом со скриптом. Так дампы не зависят от текущего каталога:
    при запуске вне Supervisor рабочий каталог может быть другим (например, на
    другом диске, где /data указывает на нужный options.json).
    """
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), name)


def describe_bytes(data, limit: int = 96) -> str:
    """
    Единый формат байтов в журнале: длина, HEX и печатный вид.
    Длинные блоки (массив общего чтения счётчика — несколько килобайт)
    показываются началом и концом, иначе журнал заливается целиком.
    """
    data = bytes(data)
    if len(data) <= limit * 2:
        return f"{len(data)} байт: {hexdump(data)} | {printable(data)}"
    return (f"{len(data)} байт (первые и последние {limit}): "
            f"{hexdump(data[:limit])} … {hexdump(data[-limit:])} | "
            f"{printable(data[:limit])} … {printable(data[-limit:])}")


def soft_parity_encode(data: bytes) -> bytes:
    """
    Повторяет логику TCP_transport.send при emulateparity=True: байт с нечётным
    числом единичных бит получает установленный 8-й бит (эмуляция 7E1 в 8N1).
    Нужна, чтобы в журнале было видно, что реально уходит в провод.
    """
    out = bytearray()
    for b in data:
        out.append(b | 0x80 if bin(b).count("1") & 1 else b)
    return bytes(out)


def mask_secret(value) -> str:
    """Позволяет отличить заданный секрет от пустого, не показывая его."""
    if value in (None, ""):
        return "(не задан)"
    return f"*** ({len(str(value))} симв.)"


def level_from_name(name, default=logging.INFO) -> int:
    """'debug' → logging.DEBUG; неизвестное значение → default."""
    if isinstance(name, int):
        return name
    return getattr(logging, str(name).strip().upper(), default)


def configure_logging(level: int) -> None:
    """
    Перенастраивает корневой логгер. Нужна потому, что basicConfig на уровне
    модуля срабатывает раньше, чем становятся известны --debug и опция log_level.
    """
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    root.addHandler(handler)
    root.setLevel(level)


def parse_cli(argv: list) -> dict:
    """Разбор ключей командной строки (для отладки вне Supervisor)."""
    flags = {"debug": False, "probe": False, "dump_array": False}
    for arg in argv:
        if arg in ("-d", "--debug"):
            flags["debug"] = True
        elif arg == "--probe":
            flags["probe"] = True
        elif arg == "--dump-array":
            flags["dump_array"] = True
        elif arg in ("-h", "--help"):
            print(USAGE)
            sys.exit(0)
        else:
            print(f"Неизвестный аргумент: {arg}\n\n{USAGE}")
            sys.exit(2)
    return flags


def build_device_info(cfg: dict, node: str) -> dict:
    """Собирает device_info из cfg['device'], отбрасывая None-поля."""
    dev = {k: v for k, v in cfg["device"].items() if v is not None}
    dev["identifiers"] = [node]
    return dev


def _monthly_energy_arg():
    """Аргумент для ENMPE: последний завершённый месяц, 'мм.гг'."""
    today = datetime.now().replace(day=1)
    prev = today - timedelta(days=1)
    return f"{prev.month:02d}.{prev.year % 100:02d}"


def _monthly_max_power_arg():
    """Аргумент для MAXPE: текущий месяц, 'мм.гг'."""
    now = datetime.now()
    return f"{now.month:02d}.{now.year % 100:02d}"


def _daily_energy_arg(days_back: int):
    """Возвращает функцию для аргумента суток на N дней назад."""
    def fn():
        d = datetime.now() - timedelta(days=days_back)
        return d.strftime("%d.%m.%y")
    return fn


def build_parameters(opts: dict) -> list:
    """
    Словарь с полями:
    * cmd - Имя параметра для счётчика (ET0PE, ENMPE, …)
    * source - "read_param" (обычный read_param) или "raw" (сырая команда)
    * arg_fn - Функция без аргументов, возвращающая строку-аргумент (только для raw)
    * key - Уникальный ключ в result (нужен, когда cmd повторяется — например, EADPE × 7)
    * topic_base - Базовый MQTT-топик (et0pe, eadpe_d0, …)
    * slow - True → читать только в «медленном» цикле
    * handler - "simple" / "tariffs" / "phases" / "archive_energy" / "archive_max_power"
    * name, device_class, state_class, unit, scale, phases, tariff_labels - Метаданные для discovery
    """
    params = []

    # ---------- БЫСТРЫЕ ----------
    if opts.get("read_energy", True):
        params.append({
            "cmd": "ET0PE", "source": "read_param",
            "key": "ET0PE", "topic_base": "et0pe",
            "slow": False, "handler": "tariffs",
            "name": "Active energy",
            "device_class": "energy", "state_class": "total_increasing",
            "unit": "kWh",
            "tariff_labels": ENERGY_TARIFF_LABELS,
        })

    if opts.get("read_power", True):
        params.append({
            "cmd": "POWEP", "source": "read_param",
            "key": "POWEP", "topic_base": "powep",
            "slow": False, "handler": "simple",
            "name": "Active power",
            "device_class": "power", "state_class": "measurement",
            "unit": "W", "scale": 1000,
        })

    if opts.get("read_voltage", True):
        params.append({
            "cmd": "VOLTA", "source": "read_param",
            "key": "VOLTA", "topic_base": "volta",
            "slow": False, "handler": "phases",
            "name": "Voltage",
            "device_class": "voltage", "state_class": "measurement",
            "unit": "V", "phases": ["A", "B", "C"],
        })

    if opts.get("read_current", True):
        params.append({
            "cmd": "CURRE", "source": "read_param",
            "key": "CURRE", "topic_base": "curre",
            "slow": False, "handler": "phases",
            "name": "Current",
            "device_class": "current", "state_class": "measurement",
            "unit": "A", "phases": ["A", "B", "C"],
        })

    # ---------- МЕДЛЕННЫЕ (АРХИВЫ) ----------
    if opts.get("read_monthly_archive", True):
        params.append({
            "cmd": "ENMPE", "source": "raw",
            "arg_fn": _monthly_energy_arg,
            "key": "ENMPE", "topic_base": "enmpe",
            "slow": True, "handler": "archive_energy",
            "name": "Energy at month start",
            "device_class": "energy", "state_class": "total_increasing",
            "unit": "kWh",
            "tariff_labels": ENERGY_TARIFF_LABELS,
        })

    if opts.get("read_daily_archive", True):
        for d in range(7):
            params.append({
                "cmd": "EADPE", "source": "raw",
                "arg_fn": _daily_energy_arg(d + 1),
                "key": f"EADPE_d{d}", "topic_base": f"eadpe_d{d}",
                "slow": True, "handler": "archive_energy",
                "name": f"Расход {d + 1} дн. назад",
                "device_class": "energy", "state_class": "total",
                "unit": "kWh",
                "tariff_labels": ENERGY_TARIFF_LABELS,
            })

    if opts.get("read_max_power", True):
        params.append({
            "cmd": "MAXPE", "source": "raw",
            "arg_fn": _monthly_max_power_arg,
            "key": "MAXPE", "topic_base": "maxpe",
            "slow": True, "handler": "archive_max_power",
            "name": "Maximum power",
            "device_class": "power", "state_class": "measurement",
            "unit": "W", "scale": 1000,
            "tariff_labels": MAX_POWER_TARIFF_LABELS,
        })

    return params


def build_cfg_from_options(opts: dict) -> dict:
    """
    Формирует cfg-словарь из опций дополнения. YAML-файл больше не используется.
    """
    return {
        "meter": {
            "host": opts.get("meter_host"),
            "port": opts.get("meter_port"),
            "address": opts.get("meter_address"),
            # Пароль нужен только в режиме session (см. open_meter).
            "password": opts.get("meter_password"),
            # fast    — быстрое выборочное чтение вне сеанса (пароль не нужен);
            # session — сессия обмена в режиме программирования.
            "read_mode": opts.get("read_mode", "fast"),
            # Отправлять быстрые кадры с адресом счётчика (/?<IDPAS>!…).
            # Нужно, если TCP-шлюз обслуживает несколько счётчиков на одной шине.
            "use_address": opts.get("use_address", False),
            # Эмуляция 7E1 в 8N1: байты с нечётной чётностью получают 8-й бит.
            # Выключайте для прозрачных 8N1-шлюзов.
            "emulate_parity": opts.get("emulate_parity", True),
        },
        "mqtt": {
            "host": opts.get("mqtt_host", "localhost"),
            "port": opts.get("mqtt_port", 1883),
            "username": opts.get("mqtt_user"),
            "password": opts.get("mqtt_password"),
            "tls": opts.get("mqtt_tls", False),
            "discovery_prefix": DISCOVERY_PREFIX,
            "topic_prefix": opts.get("topic_prefix", "energomera_ce301_meter"),
            # node_id можно задать вручную; по умолчанию — из адреса счётчика.
            "node_id": opts.get("node_id") or "meter_" + str(opts.get("meter_address", "")),
        },
        "poll": {
            "interval": opts.get("poll_interval", 60),
            "timeout": opts.get("poll_timeout", 30),
        },
        "device": {
            "name": opts.get("device_name", "Energomera Meter"),
            "model": opts.get("device_model", "Unknown"),
            "manufacturer": opts.get("device_manufacturer", "Energomera"),
            "sw_version": opts.get("device_sw_version"),
        },
    }


def load_options_from_supervisor() -> dict:
    """
    Читает /data/options.json, если файл существует.
    Возвращает плоский dict или пустой dict, если файла нет.
    """
    options_path = "/data/options.json"
    if not os.path.exists(options_path):
        log.info("No /data/options.json (running outside HA add-on?)")
        return {}
    try:
        # utf-8-sig: Supervisor пишет файл без BOM, но при ручном создании
        # (PowerShell 5.1, «Блокнот» с опцией BOM) BOM появляется и json.load
        # падает с "Unexpected UTF-8 BOM". Эта кодировка принимает оба варианта.
        with open(options_path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        log.info("Loaded %d option(s) from Supervisor", len(data))
        return data
    except Exception as e:
        log.warning("Failed to read /data/options.json: %s", e)
        return {}


def stop_handler(signum, frame):
    global running
    log.info("Received signal %s, stopping...", signum)
    running = False


def _calculate_bcc(data: bytes) -> int:
    """Вычисляет контрольную сумму (BCC) для кадра."""
    return sum(data) & 0x7F


def build_frame(command: str, address: str = None) -> bytes:
    """
    Собирает кадр быстрого выборочного чтения (инструкция, п. 5.3.2):

    /?!<SOH>R1<STX>КОМАНДА<ETX><BCC>            — безадресный
    /?<IDPAS>!<SOH>R1<STX>КОМАНДА<ETX><BCC>     — адресный
    """
    body = SOH + b'R1' + STX + command.encode('ascii') + ETX
    prefix = b'/?!' if not address else b'/?' + str(address).encode('ascii') + b'!'
    return prefix + body + bytes([_calculate_bcc(body)])


def build_session_frame(command: str) -> bytes:
    """
    Кадр внутри открытого сеанса обмена: <SOH>R1<STX>КОМАНДА<ETX><BCC>,
    без префикса `/?!` — именно так его отправляет штатный read_param()
    библиотеки и так же аддон читает параметры в read_mode: session.
    """
    body = SOH + b'R1' + STX + command.encode('ascii') + ETX
    return body + bytes([_calculate_bcc(body)])


def send_raw_command(mymeter, command: str, address: str = None) -> bytes:
    """
    Отправляет "сырую" команду на счетчик в режиме быстрого выборочного
    чтения (инструкция, п. 5.3.2), вне сеанса.

    Формат: /?!<SOH>R1<STX>COMMAND<ETX><BCC>          — безадресный
            /?<IDPAS>!<SOH>R1<STX>COMMAND<ETX><BCC>   — адресный
    """
    full_frame = build_frame(command, address)

    log.debug("Запрос %r: %s (BCC = 0x%02X)", command,
              describe_bytes(full_frame), full_frame[-1])

    # В TCP_transport.rcv() библиотеки есть дефект: остаток буфера
    # (transport.remains) не очищается, когда он короче запрошенного размера,
    # и затем примешивается к следующему ответу. На каждый запрос приходит
    # ровно один ответ, поэтому остаток перед отправкой можно смело сбросить.
    if hasattr(mymeter.transport, "remains"):
        mymeter.transport.remains = None

    started = time.monotonic()
    try:
        mymeter.transport.send(full_frame)
    except Exception as e:
        log.error("Отправка кадра %s не удалась: %s: %s",
                  hexdump(full_frame), type(e).__name__, e)
        raise

    response = mymeter.transport.recv_end(ETX)
    elapsed = time.monotonic() - started

    log.debug("Ответ на %r получен за %.3f с: %s",
              command, elapsed, describe_bytes(response))
    if not response:
        log.warning("Пустой ответ на %r: 0 байт до терминатора ETX", command)
    return response


# ---------- MQTT ----------

def build_mqtt_client(cfg: dict) -> mqtt.Client:
    mqtt_cfg = cfg["mqtt"]
    client = mqtt.Client(
        CallbackAPIVersion.VERSION2,
        client_id="energomera_ce301_bridge",
    )

    if mqtt_cfg.get("username"):
        client.username_pw_set(mqtt_cfg["username"], mqtt_cfg.get("password", ""))

    if mqtt_cfg.get("tls"):
        client.tls_set()

    client.will_set(
        f"{mqtt_cfg['topic_prefix']}/status",
        payload="offline",
        retain=True,
    )

    # В VERSION2 сигнатура on_connect другая: последний аргумент — reason_code,
    # а не rc. Сравнивать нужно с mqtt.CONNACK_ACCEPTED или с 0.
    def on_connect(c, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            log.info("MQTT connected")
            c.publish(f"{mqtt_cfg['topic_prefix']}/status", "online", retain=True)
            # Discovery публикуем при каждом подключении: иначе конфигурации
            # теряются, если брокер был недоступен в момент старта аддона,
            # и не восстанавливаются после обрыва связи.
            try:
                publish_discovery(c, cfg, cfg["parameters"])
            except Exception as e:
                log.error("Failed to publish discovery: %s", e)
        else:
            log.error("MQTT connect failed: reason_code=%s", reason_code)

    def on_disconnect(c, userdata, flags, reason_code, properties=None):
        log.warning("MQTT disconnected: reason_code=%s", reason_code)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.loop_start()

    # Брокер может быть ещё не готов — не роняем аддон при старте,
    # а ждём его появления.
    while running:
        try:
            client.connect(mqtt_cfg["host"], mqtt_cfg["port"], keepalive=60)
            break
        except Exception as e:
            log.warning(
                "MQTT connect to %s:%s failed (%s), retry in 5 s",
                mqtt_cfg["host"], mqtt_cfg["port"], e,
            )
            for _ in range(5):
                if not running:
                    break
                time.sleep(1)

    return client


# ---------- ОПРОС СЧЁТЧИКА ----------

class GatewaySilentError(Exception):
    """Шлюз не прислал ответ за отведённое время или закрыл соединение."""


class LoggingTCP_transport(TCP_transport):
    """
    TCP_transport с журналированием обмена. Закрывает два дефекта библиотеки:

    * `recv_end()` крутится в бесконечном цикле, если шлюз закрыл соединение
      (`rcv()` возвращает 0 байт) — здесь это превращается в понятную ошибку;
    * ожидание ответа никак не логировалось — теперь видны байты, тайминги и
      таймаут сокета, по которым видно, на каком шаге обмен обрывается.
    """

    def open(self):
        if self.opened:
            return
        started = time.monotonic()
        log.debug("TCP: подключение к %s:%s", self.address, self.port)
        try:
            super().open()
        except Exception as e:
            log.error(
                "TCP: не удалось подключиться к %s:%s за %.3f с: %s: %s",
                self.address, self.port, time.monotonic() - started,
                type(e).__name__, e,
            )
            raise
        log.info(
            "TCP: соединение с %s:%s установлено за %.3f с (локальный адрес %s)",
            self.address, self.port, time.monotonic() - started,
            self.sock.getsockname(),
        )
        log.debug("TCP: таймаут сокета после open = %s с, softparity = %s",
                  self.sock.gettimeout(), self.softparity)

    def send(self, data: bytes):
        wire = soft_parity_encode(data) if self.softparity else bytes(data)
        log.debug("TCP → %s (softparity=%s)", describe_bytes(wire), self.softparity)
        started = time.monotonic()
        try:
            super().send(data)
        except Exception as e:
            log.error("TCP: отправка %d байт не удалась за %.3f с: %s: %s",
                      len(data), time.monotonic() - started, type(e).__name__, e)
            raise

    def rcv(self, maxsize: int = 255):
        data = super().rcv(maxsize)          # socket.timeout пробрасываем как есть
        if not data:
            log.warning("TCP ← 0 байт: шлюз закрыл соединение")
            raise GatewaySilentError("шлюз закрыл соединение (recv вернул 0 байт)")
        log.debug("TCP ← %s", describe_bytes(data))
        return data

    def recv_end(self, End: bytes):
        timeout = self.sock.gettimeout()
        started = time.monotonic()
        log.debug("TCP: ждём ответ до терминатора 0x%s (таймаут сокета %s с)",
                  hexdump(End), timeout)
        try:
            data = super().recv_end(End)
        except socket.timeout:
            log.warning(
                "TCP: таймаут %.1f с — от %s:%s не пришло ни одного байта "
                "с терминатором 0x%s. Соединение установлено, но счётчик молчит: "
                "проверьте адрес счётчика, режим и скорость шлюза, а также что "
                "порт не занят другой программой.",
                timeout or -1, self.address, self.port, hexdump(End),
            )
            raise GatewaySilentError(f"таймаут {timeout} с: ответ не получен")
        except GatewaySilentError as e:
            log.warning("TCP: ожидание ответа прервано — %s", e)
            raise
        log.debug("TCP: кадр получен за %.3f с: %s",
                  time.monotonic() - started, describe_bytes(data))
        return data

    def close(self):
        try:
            super().close()
            log.debug("TCP: соединение с %s:%s закрыто", self.address, self.port)
        except Exception as e:
            log.debug("TCP: ошибка закрытия соединения: %s", e)


def drain_until_silence(transport, idle: float = 0.8, limit: float = 30.0) -> int:
    """
    Читает и отбрасывает всё, что приходит, пока в линии не наступит тишина
    длительностью `idle` (не дольше `limit` секунд).

    Нужно в режиме array: оборвав вычитку массива, мы оставляем счётчик
    дописывать его — шина RS-485 занята ещё примерно столько же времени, и
    хвост потока иначе будет принят за ответ на идентификацию.
    """
    dropped = 0
    started = time.monotonic()
    previous = transport.sock.gettimeout()
    try:
        while time.monotonic() - started < limit:
            transport.sock.settimeout(idle)
            try:
                data = transport.sock.recv(4096)
            except socket.timeout:
                break                      # тишина — линия свободна
            if not data:
                break
            dropped += len(data)
    finally:
        transport.sock.settimeout(previous)
    if dropped:
        log.info("Сброшено %d байт незавершённого потока за %.1f с: счётчик "
                 "докачивал массив после обрыва", dropped,
                 time.monotonic() - started)
    return dropped


def open_meter(cfg: dict) -> IEC61107:
    """
    Открывает TCP-соединение со шлюзом счётчика.

    read_mode="fast"    — быстрое выборочное чтение вне сеанса (п. 5.3.2
                          инструкции). Сессия не открывается, пароль
                          администратора не нужен.
    read_mode="session" — сессия обмена по ГОСТ Р МЭК 61107-2001 в режиме
                          программирования (поведение версии 1.0.4).
    """
    meter = cfg["meter"]
    timeout = cfg["poll"]["timeout"]

    log.debug(
        "Параметры счётчика: host=%s, port=%s, address=%r, read_mode=%s, "
        "use_address=%s, emulate_parity=%s, timeout=%s с",
        meter["host"], meter["port"], meter.get("address"), meter.get("read_mode"),
        meter.get("use_address"), meter.get("emulate_parity"), timeout,
    )

    transport = LoggingTCP_transport(
        meter["host"],
        meter["port"],
        emulateparity=meter.get("emulate_parity", True),
    )
    mymeter = IEC61107(transport)

    # Сокет открываем сами: библиотека делает это внутри init_session(),
    # а в режиме "fast" сессия не открывается вовсе.
    transport.open()

    # Таймаут ставим на сокет транспорта. У объекта IEC61107 НЕТ атрибута
    # sock, поэтому прежняя проверка hasattr(mymeter, "sock") никогда не
    # срабатывала и poll_timeout фактически не применялся.
    sock = getattr(transport, "sock", None)
    if sock is not None:
        sock.settimeout(timeout)
        log.debug("Таймаут сокета выставлен в %.1f с", sock.gettimeout())

    if meter.get("read_mode") == READ_MODE_ARRAY:
        # Сеанс обмена и идентификация. Переход в режим чтения и сам массив
        # выполняет read_all_from_array() через read_array_stream().
        #
        # Перед запросом дожидаемся тишины: после обрыва массива счётчик ещё
        # докачивает его, и хвост потока иначе был бы принят за ответ на
        # идентификацию (recv_id возвращает None, библиотека падает при разборе).
        drain_until_silence(transport)
        vendor, model = None, None
        for attempt in range(1, 4):
            try:
                vendor, model = mymeter.init_session(meter["address"])
                break
            except Exception as e:
                log.warning("Идентификация не удалась (%s: %s) — ждём тишину в "
                            "линии и пробуем снова (%d/3)",
                            type(e).__name__, e, attempt)
                drain_until_silence(transport, idle=1.0, limit=30.0)
        if not vendor:
            raise GatewaySilentError("счётчик не ответил на идентификацию")
        log.info("Идентификация счётчика: %s %s — сеанс открыт, массив будет "
                 "прочитан в режиме чтения", vendor, model)
        return mymeter

    if meter.get("read_mode") == "session":
        vendor, model = mymeter.init_session(meter["address"])
        log.debug("Identified: %s %s", vendor, model)

        # Вход в режим программирования. Счётчик разрешает его только если
        # снята аппаратная блокировка кнопкой «ДСТП» (CONDI, бит 8 = 0)
        # либо в счётчике разрешено программирование без кнопки.
        meter_id = mymeter.program_mode()
        log.debug("program_mode ok, id=%s", meter_id)

        password = meter.get("password")
        if password:
            mymeter.authorize(password)
        else:
            log.info(
                "Пароль счётчика не задан: чтение возможно, если в счётчике "
                "разрешено беспарольное выборочное чтение (CONDI, бит 0 = 0)"
            )
    else:
        log.debug(
            "Режим fast: кадры %s",
            ("адресные, /?%s!…" % meter.get("address"))
            if meter.get("use_address")
            else ("безадресные /?!… (use_address=false, адрес счётчика %r "
                  "в кадре не участвует)" % meter.get("address"))
        )

    return mymeter


def close_meter(cfg: dict, mymeter: IEC61107):
    """Закрывает сессию (в режимах session и array) и TCP-соединение."""
    read_mode = cfg["meter"].get("read_mode")
    if read_mode == "session":
        try:
            mymeter.end_session()
        except Exception as e:
            log.debug("end_session error: %s", e)
    elif read_mode == READ_MODE_ARRAY:
        # B0 отправляем только если массив был дочитан: команда, отправленная
        # посреди незакрытого потока, смешается с данными счётчика. После обрыва
        # достаточно закрыть соединение — сеанс на счётчике завершится сам.
        if getattr(mymeter, "array_complete", False):
            try:
                mymeter.end_session()
            except Exception as e:
                log.debug("end_session error: %s", e)
        else:
            log.debug("Команда B0 не отправляется: массив был оборван")
    try:
        mymeter.close()
    except Exception as e:
        log.debug("close error: %s", e)


def parse_values_response(response: bytes) -> list:
    """
    Разбирает ответ счётчика вида <STX>данные<ETX><BCC> и извлекает числа
    из круглых скобок.

    Этим способом читаются и архивные команды (ENMPE/EADPE/MAXPE), и быстрое
    выборочное чтение (ET0PE, POWEP, VOLTA, CURRE).
    """

    log.debug("Разбор ответа: %s", describe_bytes(response))

    # Ищем содержимое от STX до ETX. Вариант без ETX нужен потому, что
    # recv_end() библиотеки отбрасывает терминатор: до парсера кадр доходит
    # как <SOH>R1<STX>данные, и строгий шаблон STX..ETX его не находил.
    m = re.search(re.escape(STX) + b'(.*?)' + re.escape(ETX), response, re.DOTALL)
    if not m:
        m = re.search(re.escape(STX) + b'(.*)', response, re.DOTALL)
        if m:
            log.debug("В ответе нет ETX (его отбрасывает recv_end) — "
                      "разбираем данные до конца кадра")
    if not m:
        log.warning("Не удалось разобрать ответ счётчика (нет STX): %s",
                    describe_bytes(response))
        return []
    response_str = m.group(1).decode('ascii', errors='ignore')

    # Сообщения об ошибках имеют вид Enn (п. 5.3.3.5 инструкции)
    error_codes = re.findall(r'E(\d{2})', response_str)
    if error_codes:
        described = "; ".join(
            "E%s — %s" % (code, METER_ERRORS.get("E" + code, "неизвестная ошибка"))
            for code in error_codes
        )
        log.warning("Meter returned error: %s [%s]", response_str.strip(), described)
        return []

    log.debug("Содержимое кадра: %r", response_str)

    # Ищем все числа внутри скобок
    # Пример ответа: 1101(53309.45)(9342.39)(33230.18)...
    values = re.findall(r'\((-?[\d.]+)\)', response_str)
    if not values:
        log.warning("В ответе нет чисел в круглых скобках: %r", response_str.strip())
        return []
    return [float(v) for v in values]


def parse_array_records(array: list) -> dict:
    """
    Разбирает массив общего чтения в записи по параметрам: {имя: [[значения], …]}.

    `array` — результат `IEC61107.general_read()`: список пар [имя, значение],
    где значение — строка из скобок. Значения, которые не являются числами
    (например `GRF00` со значением `0:0:2`), пропускаются: они аддону не нужны.
    """
    flat = {}
    for name, raw in array:
        size = ARRAY_RECORD_SIZE.get(name)
        if not size:
            continue
        try:
            value = float(str(raw).replace(",", "."))
        except (TypeError, ValueError):
            continue
        flat.setdefault(name, []).append(value)

    records = {}
    for name, values in flat.items():
        size = ARRAY_RECORD_SIZE[name]
        records[name] = [values[i:i + size]
                         for i in range(0, len(values), size)]
        log.debug("Массив: %s — %d значений, %d записей по %d",
                  name, len(values), len(records[name]), size)
    return records


def values_from_array(array: list) -> dict:
    """
    Преобразует массив общего чтения в значения по ключам параметров аддона —
    так же, как это делает read_all() для отдельных запросов.

    Месячный архив счётчик отдаёт кольцевым буфером: порядок записей «повёрнут»
    относительно календаря. Восстановить его можно по накопленной энергии — она
    монотонно растёт, поэтому последний завершённый месяц это запись с
    наибольшим итогом, не превышающим текущий. Максимумы мощности лежат в таком
    же кольце из 12 месяцев и сдвигаются синхронно, поэтому берутся из той же
    записи.

    Суточный архив (EADPE) упорядочить по данным нельзя: суточный расход не
    монотонен, а сдвиг кольца в значениях не выражен. В этом режиме он не
    публикуется (исключается из опроса в main).
    """
    records = parse_array_records(array)
    values = {}

    for name in ("ET0PE", "POWEP", "VOLTA", "CURRE"):
        if records.get(name):
            values[name] = records[name][0]

    months = records.get("ENMPE") or []
    month_index = None
    if months:
        total = values.get("ET0PE", [None])[0]
        if total is None:
            month_index = len(months) - 1
            log.warning("Нет текущей энергии в массиве — берём последнюю запись "
                        "месячного архива")
        else:
            candidates = [i for i, m in enumerate(months) if m[0] <= total]
            if candidates:
                month_index = max(candidates, key=lambda i: months[i][0])
            else:
                month_index = min(range(len(months)), key=lambda i: months[i][0])
                log.warning("Все записи месячного архива больше текущей энергии "
                            "(%s) — берём минимальную", total)
        values["ENMPE"] = months[month_index]
        log.info("Месячный архив (кольцевой буфер, записей %d): выбрана запись "
                 "№%d с энергией %s как последний завершённый месяц",
                 len(months), month_index, months[month_index][0])

    maxima = records.get("MAXPE") or []
    if maxima and month_index is not None and month_index < len(maxima):
        values["MAXPE"] = maxima[month_index]
        log.debug("Максимумы мощности взяты из записи №%d: %s",
                  month_index, maxima[month_index])
    return values


def array_needed_values(cfg: dict) -> dict:
    """Сколько значений каждого параметра нужно собрать по включённым флагам."""
    needed = {}
    for p in cfg["parameters"]:
        name = p["cmd"]
        if name in ARRAY_NEEDED_VALUES:
            needed[name] = ARRAY_NEEDED_VALUES[name]
    return needed


def array_needs_satisfied(text: bytes, needed: dict) -> bool:
    """
    Собраны ли уже все нужные параметры. Дополнительно требуем, чтобы буфер
    заканчивался переводом строки: иначе последнее значение может быть обрезано
    (`23` вместо `234.11`).
    """
    if not text.endswith(b"\n"):
        return False
    counts = {}
    for m in re.finditer(rb"([A-Z][A-Z0-9_]{1,9})\(", text):
        name = m.group(1).decode("ascii")
        if name in needed:
            counts[name] = counts.get(name, 0) + 1
    return all(counts.get(name, 0) >= count for name, count in needed.items())


def read_array_stream(mymeter, needed: dict, timeout: float):
    """
    Читает массив общего чтения, но не обязательно до конца: как только собраны
    все нужные параметры, поток обрывается.

    Замеры на CE301 v11 показывают, почему «частый» цикл короче 51 % не сделать:
    `ET0PE` приходит в начале потока (0 %), `ENMPE` — на 1 %, `EADPE` — на 31 %,
    `MAXPE` — на 45 %, а мощность, ток и напряжение — только на 51 %. То есть
    максимумы мощности приходят РАНЬШЕ токов и напряжений, и отложить их «на час»
    без потери оперативных данных нельзя. Зато обрыв на 51 % вдвое сокращает
    цикл (≈21 с вместо ≈42 с) и отдаёт при этом все публикуемые параметры:
    остаток массива — служебные `GRF`, `CHS`, `LISTR`.

    Возвращает (array, complete, bytes_read, elapsed).
    """
    transport = mymeter.transport
    # В TCP_transport.rcv() остаток буфера не очищается, когда он короче
    # запрошенного размера, и примешивается к следующему ответу.
    if hasattr(transport, "remains"):
        transport.remains = None

    # ACK с переходом в режим чтения: 0 <код скорости> 0 <CR><LF>
    mymeter.read_data()

    started = time.monotonic()
    buf = bytearray()
    complete = False
    while time.monotonic() - started < timeout:
        try:
            chunk = transport.rcv(256)
        except socket.timeout:
            log.warning("Массив общего чтения прерван по таймауту на %d байт",
                        len(buf))
            break
        except Exception as e:
            log.warning("Чтение массива прервано: %s: %s", type(e).__name__, e)
            break
        buf += chunk
        if ETX in buf:
            complete = True
            break
        if array_needs_satisfied(bytes(buf), needed):
            break

    elapsed = time.monotonic() - started
    text = bytes(buf)
    if STX in text:
        text = text.split(STX, 1)[1]
    text = text.split(ETX, 1)[0]
    array = [(m.group(1).decode("ascii"), m.group(2).decode("ascii"))
             for m in re.finditer(rb"([A-Z][A-Z0-9_]{1,9})\(([^)]*)\)", text)]
    return array, complete, len(buf), elapsed


def read_all_from_array(cfg: dict, mymeter) -> dict:
    """
    Читает массив общего чтения (read_mode: array) и разбирает его.

    Если счётчик отдал меньше записей архива, чем ожидается (например, у нового
    счётчика ещё нет 12 месяцев), условие «нужные параметры собраны» не
    выполнится и массив будет дочитан до конца — данные важнее скорости.
    """
    needed = array_needed_values(cfg)
    log.debug("Ожидаем из массива: %s",
              ", ".join("%s×%d" % (n, c) for n, c in sorted(needed.items())))

    array, complete, size, elapsed = read_array_stream(
        mymeter, needed, cfg["poll"]["timeout"])
    mymeter.array_complete = complete
    log.info("Массив общего чтения: %d строк, %d байт за %.1f с (%s)",
             len(array), size, elapsed,
             "дочитан до конца" if complete else "оборван на нужных параметрах")
    if not array:
        log.warning("Массив общего чтения пуст — счётчик не отдал данные")
        return {}

    names = {name for name, _ in array}
    log.debug("Нужные аддону параметры в массиве: %s",
              ", ".join(sorted(names & set(ARRAY_RECORD_SIZE))) or "нет")

    values = values_from_array(array)
    for key in sorted(values):
        log.info("Read %s = %r", key, values[key])
    return values


def read_all(cfg: dict, mymeter, read_slow_data: bool = True) -> dict:
    """Читает параметры: в режиме array — весь массив, иначе по запросам."""
    if cfg["meter"].get("read_mode") == READ_MODE_ARRAY:
        return read_all_from_array(cfg, mymeter)

    result = {}
    meter = cfg["meter"]
    session_mode = meter.get("read_mode") == "session"
    # Адрес подставляется в кадр только при use_address=true: часть TCP-шлюзов
    # обслуживает несколько счётчиков на шине и требует адресный кадр.
    address = meter.get("address") if meter.get("use_address") else None

    planned = [p for p in cfg["parameters"]
               if not (p.get("slow") and not read_slow_data)]
    log.debug("К чтению %d параметр(ов): %s", len(planned),
              ", ".join(p["cmd"] for p in planned))

    for p in planned:
        key = p.get("key", p["cmd"])
        started = time.monotonic()
        try:
            if session_mode and p.get("source") != "raw":
                # Сессионное чтение штатным методом библиотеки
                value = mymeter.read_param(p["cmd"])
            else:
                # Быстрое выборочное чтение вне сеанса (п. 5.3.2 инструкции):
                #   /?!<SOH>R1<STX>NAME(аргумент)<ETX><BCC>
                arg_fn = p.get("arg_fn")
                arg = arg_fn() if arg_fn else ""
                command = f"{p['cmd']}({arg})" if arg else f"{p['cmd']}()"
                response = send_raw_command(mymeter, command, address)
                value = parse_values_response(response)

            result[key] = value
            log.info("Read %s (%s) = %r за %.3f с",
                     p["cmd"], key, value, time.monotonic() - started)
        except Exception as e:
            result[key] = None
            log.warning("Failed to read %s (%s) за %.3f с: %s: %s",
                        p["cmd"], key, time.monotonic() - started,
                        type(e).__name__, e)
            log.debug("Трассировка ошибки чтения %s:", p["cmd"], exc_info=True)
    return result


# ---------- ПУБЛИКАЦИЯ ----------

def parse_value(raw):
    """
    IEC61107 иногда возвращает значения как ('1.234', 'kWh') или кортеж.
    Приводим к float, если возможно.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)
    if isinstance(raw, (list, tuple)):
        # берём первое числовое значение
        for item in raw:
            try:
                return float(str(item).replace(",", "."))
            except (ValueError, TypeError):
                continue
        return None
    try:
        return float(str(raw).replace(",", "."))
    except (ValueError, TypeError):
        return None


def publish_values(client: mqtt.Client, cfg: dict, values: dict):
    topic_prefix = cfg["mqtt"]["topic_prefix"]
    published = 0

    for p in cfg["parameters"]:
        key = p.get("key", p["cmd"])
        raw = values.get(key)
        if raw is None or raw == []:
            log.debug("Пропуск публикации %s (%s): значение %r", p["cmd"], key, raw)
            continue

        handler = p.get("handler", "simple")
        topic_base = p.get("topic_base", key.lower())
        scale = p.get("scale", 1)

        if handler == "simple":
            val = parse_value(raw)
            if val is None:
                log.warning("Не удалось привести к числу %s (%s): %r", p["cmd"], key, raw)
                continue
            client.publish(f"{topic_prefix}/{topic_base}",
                           val * scale, retain=True)
            published += 1

        elif handler == "phases":
            labels = [(lbl.lower(), lbl) for lbl in p.get("phases", [])]
            published += _publish_list(client, topic_prefix, topic_base, raw, labels, scale)

        elif handler in ("tariffs", "archive_energy", "archive_max_power"):
            labels = p.get("tariff_labels", [])
            published += _publish_list(client, topic_prefix, topic_base, raw, labels, scale)

        else:
            log.warning("Неизвестный обработчик %r у параметра %s", handler, p["cmd"])

    log.info("Опубликовано значений: %d (прочитано ключей: %d)", published, len(values))
    if published == 0 and values:
        log.warning(
            "Ни одно значение не опубликовано: все прочитанные данные пусты. "
            "Смотрите предупреждения о разборе ответа выше — значения не найдены "
            "в ответе счётчика."
        )


def _publish_list(client, topic_prefix, topic_base, raw, labels, scale) -> int:
    """Раскладывает список значений по топикам {topic_base}/{suffix}."""
    seq = raw if isinstance(raw, (list, tuple)) else [raw]
    nums = [parse_value(x) for x in seq]

    published = 0
    for idx, (suffix, _label) in enumerate(labels):
        if idx >= len(nums) or nums[idx] is None:
            continue
        client.publish(f"{topic_prefix}/{topic_base}/{suffix}",
                       nums[idx] * scale, retain=True)
        published += 1

    if published < len(labels):
        log.debug("Из %d суффиксов %s опубликовано %d: пришло значений %d",
                  len(labels), topic_base, published, len(nums))
    return published


def publish_discovery(client: mqtt.Client, cfg: dict, params: list):
    mqtt_cfg = cfg["mqtt"]
    prefix = mqtt_cfg["discovery_prefix"]
    node = mqtt_cfg["node_id"]
    topic_prefix = mqtt_cfg["topic_prefix"]

    device_info = build_device_info(cfg, node)

    for p in params:
        handler = p.get("handler", "simple")
        topic_base = p.get("topic_base", p["cmd"].lower())
        name = p.get("name", p["cmd"])

        # Собираем список (suffix, label) — как в publish_values
        if handler in ("tariffs", "archive_energy", "archive_max_power"):
            items = p.get("tariff_labels", [])
        elif handler == "phases":
            items = [(lbl.lower(), lbl) for lbl in p.get("phases", [])]
        else:
            items = [("", "")]  # одиночная сущность

        for suffix, label in items:
            if suffix:
                uid = f"{node}_{topic_base}_{suffix}"
                cfg_topic = f"{prefix}/sensor/{node}/{topic_base}_{suffix}/config"
                state_topic = f"{topic_prefix}/{topic_base}/{suffix}"
                entity_name = f"{name} {label}"
            else:
                uid = f"{node}_{topic_base}"
                cfg_topic = f"{prefix}/sensor/{node}/{topic_base}/config"
                state_topic = f"{topic_prefix}/{topic_base}"
                entity_name = name

            payload = {
                "name": entity_name,
                "unique_id": uid,
                "state_topic": state_topic,
                "device_class": p.get("device_class"),
                "state_class": p.get("state_class"),
                "unit_of_measurement": p.get("unit"),
                # Доступность: аддон публикует online при подключении к брокеру
                # и offline при сбое цикла опроса либо по Last Will.
                "availability_topic": f"{topic_prefix}/status",
                "payload_available": "online",
                "payload_not_available": "offline",
                "device": device_info,
            }
            client.publish(cfg_topic, json.dumps(payload), retain=True)

    log.info("Published discovery configs for %d parameter(s)", len(params))


# ---------- ДИАГНОСТИКА КАНАЛА (--probe) ----------

def probe_read(sock, terminator: bytes, timeout: float):
    """
    Читает из сокета, пока не встретит терминатор или не истечёт таймаут.

    Бит чётности снимается (`byte & 0x7F`) так же, как это делает
    TCP_transport.rcv(): счётчик отвечает в 7E1, и без снятия бита терминатор
    `<CR><LF>` приходит как `8D 0A` и не находится.

    `terminator=None` — читать всё, что придёт, до истечения таймаута
    (пассивное прослушивание: вдруг шлюз сам присылает баннер).
    Возвращает (data, error), где error: None — терминатор найден,
    "timeout" — данные не пришли, "closed" — шлюз закрыл соединение.
    В отличие от библиотеки, чтение всегда заканчивается: без терминатора
    возвращается то, что успело прийти.
    """
    buf = bytearray()
    deadline = time.monotonic() + timeout
    while True:
        left = deadline - time.monotonic()
        if left <= 0:
            return bytes(buf), "timeout"
        sock.settimeout(left)
        try:
            chunk = sock.recv(256)
        except socket.timeout:
            return bytes(buf), "timeout"
        if not chunk:
            return bytes(buf), "closed"
        log.debug("  сырые байты из сокета: %s", describe_bytes(chunk))
        buf += bytes(b & 0x7F for b in chunk)
        if terminator and terminator in buf:
            return bytes(buf), None


def probe_after_drain(sock, timeout: float) -> dict:
    """
    После полной вычитки массива проверяет, отвечает ли счётчик на отдельные
    запросы архивных параметров. От этого зависит, можно ли брать суточный
    архив по датам: в самом массиве суточные записи лежат кольцевым буфером и
    по значениям их порядок не восстановить (в отличие от месячной энергии,
    которая монотонно растёт).
    """
    frames = [
        ("ENMPE(%s)" % _monthly_energy_arg(),
         "энергия на конец предыдущего месяца"),
        ("EADPE(%s)" % _daily_energy_arg(1)(),
         "расход за предыдущие сутки"),
        ("MAXPE(%s)" % _monthly_max_power_arg(),
         "максимум мощности за текущий месяц"),
    ]
    result = {}

    # По ГОСТ Р МЭК 61107 после приёма блока данных клиент обязан подтвердить его
    # символом ACK (0x06). Библиотека этого не делает — проверяем, не из-за этого
    # ли счётчик игнорирует последующие запросы.
    log.info("  → ACK принятого массива (0x06)")
    try:
        sock.sendall(soft_parity_encode(b"\x06"))
        time.sleep(0.3)
    except Exception as e:
        log.warning("    не удалось отправить ACK: %s: %s", type(e).__name__, e)

    for command, title in frames:
        wire = soft_parity_encode(build_session_frame(command))
        log.info("  → после массива: %s (%s)", command, title)
        log.info("    отправлено: %s", describe_bytes(wire))
        try:
            sock.sendall(wire)
        except Exception as e:
            log.warning("    отправка не удалась: %s: %s", type(e).__name__, e)
            break
        started = time.monotonic()
        data, error = probe_read(sock, ETX, timeout)
        if error is None:
            result[command] = data
            log.info("    ← ОТВЕТ за %.1f с: %s",
                     time.monotonic() - started, describe_bytes(data))
        else:
            log.warning("    ← ответа нет за %.1f с (%s): %s",
                        time.monotonic() - started, error, describe_bytes(data))
    return result


def run_dump_array(cfg: dict) -> int:
    """
    Снимает массив общего чтения и показывает порядок параметров в потоке:
    сколько байт нужно вычитать, чтобы добраться до конкретного параметра.

    Это определяет, можно ли оборвать вычитку массива, не дожидаясь конца:
    если нужные для частого опроса параметры идут в начале, быстрый цикл
    занимает секунды вместо десятков секунд.
    """
    meter = cfg["meter"]
    host, port = meter["host"], meter["port"]
    timeout = max(cfg["poll"]["timeout"], 120.0)
    address = str(meter.get("address") or "")

    log.info("=" * 72)
    log.info("DUMP: снимаем массив общего чтения с %s:%s (адрес %r)",
             host, port, address)
    log.info("=" * 72)

    transport = LoggingTCP_transport(host, port,
                                     emulateparity=meter.get("emulate_parity", True))
    mymeter = IEC61107(transport)
    raw, error = b"", None
    try:
        transport.open()
        transport.sock.settimeout(timeout)
        vendor, model = mymeter.init_session(address)
        log.info("Идентификация: %s %s, код скорости %r", vendor, model,
                 mymeter.baudrate)
        ack = b"\x06" + b"0" + str(mymeter.baudrate).encode("ascii") + b"0" + b"\r\n"
        log.info("→ ACK режима чтения: %s", describe_bytes(soft_parity_encode(ack)))
        transport.send(ack)

        started = time.monotonic()
        raw, error = probe_read(transport.sock, ETX, timeout)
        elapsed = time.monotonic() - started
    except Exception as e:
        log.error("DUMP: сеанс не удался за %.1f с: %s: %s",
                  time.monotonic() - started, type(e).__name__, e)
        return 1
    finally:
        try:
            transport.close()
        except Exception:
            pass

    if not raw:
        log.error("DUMP: счётчик не отдал массив (%s)", error)
        return 1

    text = raw.split(STX, 1)[-1]
    complete = ETX in raw
    dump_path = script_dir_path("array_dump.log")
    with open(dump_path, "wb") as f:
        f.write(text)
    log.info("Массив: %d байт за %.1f с, %s; сохранён в %s",
             len(text), elapsed, "полный" if complete else "НЕПОЛНЫЙ (оборван)",
             dump_path)

    # Считаем, на каком байте потока впервые встречается каждый параметр.
    offset = 0
    first = {}
    counts = {}
    for line in text.decode("ascii", errors="ignore").splitlines():
        m = re.match(r"([A-Z][A-Z0-9_]{1,9})\(", line)
        if m:
            name = m.group(1)
            counts[name] = counts.get(name, 0) + 1
            first.setdefault(name, offset)
        offset += len(line) + 2  # + CRLF

    log.info("Порядок нужных аддону параметров в потоке (доля прочитанного):")
    for name in sorted(ARRAY_RECORD_SIZE, key=lambda n: first.get(n, 1 << 30)):
        if name not in first:
            log.info("  %-6s — в массиве не найден", name)
            continue
        log.info("  %-6s — впервые на байте %6d из %6d (%.0f%% потока), "
                 "значений %d", name, first[name], len(text),
                 100.0 * first[name] / max(len(text), 1), counts[name])
    log.info("DUMP: быстрый цикл возможен, если нужные ему параметры идут "
             "в начале потока")
    return 0


def probe_session(host: str, port, address: str, timeout: float, mode: str) -> dict:
    """
    Одна попытка сеанса обмена: подключение → идентификация → ACK с переходом
    в режим → запрос ET0PE. `mode`: '0' — режим чтения, '1' — программирования.

    Нужна, когда шлюз игнорирует быстрое выборочное чтение вне сеанса: видно,
    отвечает ли счётчик на идентификацию и отдаёт ли параметр внутри сеанса.
    """
    mode_name = "чтение (0)" if mode == "0" else "программирование (1)"
    log.info("— сеанс обмена, режим %s", mode_name)
    result = {"received": 0, "identified": False, "ack": False, "general": False,
              "param": False, "ident": "", "data": b"", "targeted": {}}

    try:
        sock = socket.create_connection((host, port), timeout=timeout)
    except Exception as e:
        log.warning("  TCP-соединение не установлено: %s: %s", type(e).__name__, e)
        return result

    try:
        request = soft_parity_encode(b"/?" + address.encode("ascii") + b"!\r\n")
        log.info("  → идентификация: %s", describe_bytes(request))
        sock.sendall(request)
        data, error = probe_read(sock, b"\r\n", timeout)
        result["received"] += len(data)
        if error:
            log.warning("  ← ответа на идентификацию нет (%s): %s",
                        error, describe_bytes(data))
            return result
        log.info("  ← идентификация: %s", describe_bytes(data))
        ident = data.decode("ascii", errors="ignore").strip()
        result["identified"] = True
        result["ident"] = ident
        # Формат ответа: '<'/'>' + 3 символа производителя + код скорости + ID
        baud = ident[4:5] if len(ident) > 4 else "5"
        log.info("  счётчик: производитель %r, код скорости %r, ID %r",
                 ident[1:4], baud, ident[5:])

        ack = b"\x06" + b"0" + baud.encode("ascii") + mode.encode("ascii") + b"\r\n"
        log.info("  → ACK с переходом в режим: %s", describe_bytes(soft_parity_encode(ack)))
        sock.sendall(soft_parity_encode(ack))

        # После ACK счётчик отдаёт массив общего чтения — у CE301 это больше
        # 10 КБ и приходит медленно (порядка сотен байт в секунду), поэтому
        # только на вычитку массива даём не меньше минуты. Его обязательно
        # дочитать до ETX: иначе следующий ответ будет хвостом этого массива и
        # его легко принять за ответ на параметр.
        drain_timeout = max(timeout, 120.0)
        started = time.monotonic()
        data, error = probe_read(sock, ETX, drain_timeout)
        elapsed = time.monotonic() - started
        result["received"] += len(data)
        if data:
            result["ack"] = True
            result["general"] = ETX in data
            log.info("  ← после ACK за %.1f с: %s", elapsed, describe_bytes(data))
            if ETX not in data:
                log.warning("  ← массив общего чтения не закончился и за %.0f с "
                            "(ETX не найден); запрос параметра пропускаем, чтобы "
                            "не спутать его хвост с ответом", drain_timeout)
            # Дамп сохраняем даже частичным: по нему видно, что счётчик отдаёт
            # сам, без отдельных запросов параметров.
            dump_path = script_dir_path(f"probe_session_{mode}.log")
            try:
                with open(dump_path, "wb") as f:
                    f.write(data)
                    if ETX not in data:
                        f.write(b"\r\n### DUMP INCOMPLETE: ETX not found in %.0f s\r\n"
                                % drain_timeout)
                names = re.findall(r"([A-Z][A-Z0-9_]{1,9})\(",
                                   data.decode("ascii", errors="ignore"))
                log.info("  массив сохранён в %s (%d байт, %s), уникальных имён "
                         "параметров: %d", dump_path, len(data),
                         "полный" if ETX in data else "ЧАСТИЧНЫЙ",
                         len(set(names)))
                log.info("  имена: %s", ", ".join(sorted(set(names))))
            except Exception as e:
                log.warning("  не удалось сохранить дамп: %s", e)
        else:
            log.info("  ← после ACK тишина (%s): счётчик переход в режим "
                     "не подтвердил", error)

        if not (data and ETX in data):
            if data:
                result["data"] = data
            return result

        # Пробуем оба варианта: внутри сеанса кадр идёт без префикса `/?!`,
        # но некоторые шлюзы принимают и «быстрый» вариант.
        if mode == "0":
            # Массив вычитан полностью — проверяем отдельные архивные запросы.
            result["targeted"] = probe_after_drain(sock, timeout)

        for label, wire in (
            ("кадр внутри сеанса <SOH>R1<STX>ET0PE()<ETX><BCC>",
             soft_parity_encode(build_session_frame("ET0PE()"))),
            ("кадр быстрого чтения /?!<SOH>R1<STX>ET0PE()<ETX><BCC>",
             soft_parity_encode(build_frame("ET0PE()"))),
        ):
            log.info("  → запрос ET0PE: %s (%s)", describe_bytes(wire), label)
            sock.sendall(wire)
            data, error = probe_read(sock, ETX, timeout)
            result["received"] += len(data)
            result["data"] = data
            if error is None:
                result["param"] = True
                log.info("  ← ОТВЕТ на ET0PE: %s", describe_bytes(data))
                break
            log.warning("  ← ответа на ET0PE нет (%s): %s", error, describe_bytes(data))
    except Exception as e:
        log.warning("  обмен прерван: %s: %s", type(e).__name__, e)
    finally:
        try:
            sock.close()
        except Exception:
            pass

    return result


def run_probe(cfg: dict) -> int:
    """
    Автономная диагностика канала до счётчика: без MQTT и без библиотеки.

    Проверяет TCP-доступность шлюза, пассивно слушает порт, перебирает
    варианты кадра чтения (безадресный/адресный, 7E1 и 8N1), «провоцирует»
    счётчик запросами идентификации и отдельными подключениями проверяет
    сеансовый режим. По результату видно, что именно понимает счётчик и какую
    опцию аддона включить.
    """
    meter = cfg["meter"]
    host, port = meter["host"], meter["port"]
    timeout = cfg["poll"]["timeout"]
    address = str(meter.get("address") or "")

    log.info("=" * 72)
    log.info("PROBE: шлюз %s:%s, адрес счётчика %r, таймаут %.1f с",
             host, port, address, timeout)
    log.info("=" * 72)

    started = time.monotonic()
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
    except Exception as e:
        log.error("PROBE 1/2: TCP-соединение НЕ установлено за %.3f с: %s: %s",
                  time.monotonic() - started, type(e).__name__, e)
        log.error("PROBE: проверьте meter_host/meter_port, питание и режим шлюза, "
                  "а также что порт не занят другой программой (AdminTools и т. п.)")
        return 1

    log.info("PROBE 1/3: TCP-соединение установлено за %.3f с (локальный %s, шлюз %s)",
             time.monotonic() - started, sock.getsockname(), sock.getpeername())

    received = 0

    # Пассивное прослушивание: часть шлюзов присылает баннер или накопленные
    # данные сразу после подключения, до любого запроса.
    log.info("PROBE 2/3: пассивное прослушивание 3 с — мы ещё ничего не отправляли")
    data, error = probe_read(sock, None, 3.0)
    received += len(data)
    if data:
        log.info("  шлюз сам прислал: %s", describe_bytes(data))
    else:
        log.info("  тишина (%s): шлюз обмен не инициирует", error)

    variants = [
        ("безадресный кадр, эмуляция чётности 7E1 (текущее поведение аддона)",
         build_frame("ET0PE()"), True, False),
        ("адресный кадр, эмуляция чётности 7E1",
         build_frame("ET0PE()", address), True, True),
        ("безадресный кадр, «как есть» 8N1",
         build_frame("ET0PE()"), False, False),
        ("адресный кадр, «как есть» 8N1",
         build_frame("ET0PE()", address), False, True),
    ]

    log.info("PROBE 3/3: перебор вариантов кадра чтения ET0PE")
    worked = []
    fast_plain = fast_addressed = fast_7e1 = fast_8n1 = False
    for title, frame, parity, addressed in variants:
        wire = soft_parity_encode(frame) if parity else frame
        log.info("— %s", title)
        log.info("  отправлено: %s", describe_bytes(wire))
        try:
            sock.sendall(wire)
        except Exception as e:
            log.warning("  отправка не удалась: %s: %s", type(e).__name__, e)
            break

        started = time.monotonic()
        data, error = probe_read(sock, ETX, timeout)
        elapsed = time.monotonic() - started
        received += len(data)
        if error is None:
            log.info("  ОТВЕТ за %.3f с: %s", elapsed, describe_bytes(data))
            worked.append(("fast", title))
            fast_addressed = fast_addressed or addressed
            fast_plain = fast_plain or not addressed
            fast_7e1 = fast_7e1 or parity
            fast_8n1 = fast_8n1 or not parity
        else:
            log.warning("  ответа нет за %.3f с (%s): %s",
                        elapsed, error, describe_bytes(data))
            if error == "closed":
                log.warning("  шлюз закрыл соединение — вероятно, кадр им отвергнут")
                break

    # Короткие «провокации»: живой счётчик отвечает на запрос идентификации,
    # даже если не принимает кадры выборочного чтения.
    provocations = [
        ("raw", "перевод строки <CR><LF>", b"\r\n", b"\r\n"),
        ("ident", "безадресный запрос идентификации /?!<CR><LF>",
         b"/?!\r\n", b"\r\n"),
        ("ident", "адресный запрос идентификации /?%s!<CR><LF>" % address,
         b"/?" + address.encode("ascii") + b"!\r\n", b"\r\n"),
    ]
    identified = False
    for kind, title, request, terminator in provocations:
        log.info("— провокация: %s", title)
        log.info("  отправлено: %s", describe_bytes(soft_parity_encode(request)))
        try:
            sock.sendall(soft_parity_encode(request))
        except Exception as e:
            log.warning("  отправка не удалась: %s: %s", type(e).__name__, e)
            break

        started = time.monotonic()
        data, error = probe_read(sock, terminator, timeout)
        received += len(data)
        if error is None:
            log.info("  ОТВЕТ за %.3f с: %s",
                     time.monotonic() - started, describe_bytes(data))
            worked.append((kind, title))
            if kind == "ident":
                identified = True
        else:
            log.warning("  ответа нет за %.3f с (%s): %s",
                        time.monotonic() - started, error, describe_bytes(data))
            if error == "closed":
                log.info("  соединение закрыто шлюзом — это уже реакция на наши байты")
                break

    try:
        sock.close()
    except Exception:
        pass

    # Сеансовый режим проверяем отдельными подключениями: счётчик может
    # поддерживать только его.
    log.info("PROBE: проверка сеансового режима (идентификация → ACK → ET0PE)")
    sessions = {}
    for mode in ("0", "1"):
        sessions[mode] = probe_session(host, port, address, timeout, mode)
        received += sessions[mode]["received"]

    log.info("=" * 72)
    fast_ok = [title for kind, title in worked if kind == "fast"]
    ident_ok = identified or any(r["identified"] for r in sessions.values())
    session_ok = [mode for mode, r in sessions.items() if r["param"]]
    general_ok = [mode for mode, r in sessions.items() if r["general"]]

    if fast_ok:
        log.info("PROBE: быстрое выборочное чтение работает: %s", "; ".join(fast_ok))
        if fast_addressed and not fast_plain:
            log.info("PROBE: отвечает только адресный кадр → "
                     "включите опцию use_address: true")
        if fast_8n1 and not fast_7e1:
            log.info("PROBE: отвечает только вариант 8N1 → "
                     "установите emulate_parity: false")
        return 0

    if session_ok:
        log.info("PROBE: быстрое чтение вне сеанса счётчик игнорирует, но в "
                 "сеансовом режиме ответ на ET0PE получен (режимы: %s) → "
                 "установите read_mode: session", ", ".join(session_ok))
        if "0" in session_ok and "1" not in session_ok:
            log.info("PROBE: параметр читается в сеансе БЕЗ режима "
                     "программирования (режим 0). Аддон в read_mode: session "
                     "входит в режим программирования (режим 1), а счётчик его "
                     "не пускает, хотя для чтения он не нужен.")
        return 0

    if ident_ok:
        log.info("PROBE: счётчик отвечает на идентификацию сеанса — шлюз, линия "
                 "и адрес в порядке. Похоже на ограничение доступа, а не связи:")
        if general_ok:
            log.info("  массив общего чтения получен в режимах: %s — сеанс, режим "
                     "и связь со счётчиком работают.", ", ".join(general_ok))
            targeted = sessions.get("0", {}).get("targeted") or {}
            answered = [cmd for cmd, data in targeted.items() if data]
            if answered:
                log.info("  отдельные архивные запросы после массива отвечают: %s",
                         ", ".join(answered))
            elif targeted:
                log.info("  отдельные архивные запросы после массива не отвечают — "
                         "архивы придётся разбирать из самого массива.")
            log.info("  похоже, для этой связки подходит read_mode: array — аддон "
                     "сам вычитает массив и разбирает его, а месяц в кольцевом "
                     "архиве определяет по накопленной энергии.")
        log.error("  • в счётчике может быть запрещено беспарольное выборочное "
                  "чтение (CONDI, бит 0 = 1) — нужен пароль;")
        log.error("  • вход в режим программирования закрыт аппаратной "
                  "блокировкой кнопкой «ДСТП» (CONDI, бит 8 = 0);")
        log.error("  • параметр ET0PE может быть недоступен на текущем уровне "
                  "доступа (таблица А.3 инструкции).")
        log.error("PROBE: подробности по шагам сеанса — в журнале выше "
                  "(строки «← после ACK» и «← ответа на ET0PE нет»).")
        return 1

    log.error("PROBE: ответа нет ни на один вариант, и за весь тест от %s:%s "
              "пришло %d байт. TCP-соединение устанавливается и не "
              "закрывается — слушает кто-то живой, но на наши кадры он никак "
              "не реагирует. Вероятные причины:", host, port, received)
    log.error("  • это не тот порт или не то устройство: сверьтесь с "
              "веб-интерфейсом шлюза;")
    log.error("  • шлюз работает не прозрачно, а по своему протоколу; "
              "посмотрите его режим (TCP-сервер / TCP-клиент / UDP);")
    log.error("  • последовательная сторона шлюза мертва: не совпадают скорость "
              "и 7E1/8N1, обрыв линии, нет питания интерфейса счётчика;")
    log.error("  • шлюз в режиме TCP-клиента и должен сам подключаться к "
              "приёмнику данных, а не ждать нас;")
    log.error("  • порт занят другой программой: TCP-шлюзы обычно держат "
              "одного клиента.")
    return 1


# ---------- MAIN ----------

def log_effective_config(cfg: dict, interval, slow_interval) -> None:
    """
    Печатает действующую конфигурацию без секретов: видно, что реально
    пришло из /data/options.json (частая причина — не тот хост или порт).
    """
    meter, mqtt = cfg["meter"], cfg["mqtt"]
    log.info(
        "Конфигурация: счётчик %s:%s, адрес %r, read_mode=%s, use_address=%s, "
        "emulate_parity=%s, timeout=%s с",
        meter["host"], meter["port"], meter.get("address"), meter.get("read_mode"),
        meter.get("use_address"), meter.get("emulate_parity"),
        cfg["poll"]["timeout"],
    )
    log.info(
        "Конфигурация: MQTT %s:%s, user=%s, password=%s, tls=%s, "
        "topic_prefix=%s, node_id=%s",
        mqtt["host"], mqtt["port"], mqtt.get("username") or "(не задан)",
        mask_secret(mqtt.get("password")), mqtt.get("tls"),
        mqtt["topic_prefix"], mqtt["node_id"],
    )
    log.info(
        "Конфигурация: пароль счётчика %s, интервалы %s с / %s с, "
        "параметров %d (%s)",
        mask_secret(meter.get("password")), interval, slow_interval,
        len(cfg["parameters"]), ", ".join(p["cmd"] for p in cfg["parameters"]),
    )
    log.info("Уровень журнала: %s",
             logging.getLevelName(logging.getLogger().level))


def main():
    flags = parse_cli(sys.argv[1:])

    # Приоритет уровня журнала: --debug → переменная окружения → опция аддона.
    level_name = os.environ.get("METER_BRIDGE_LOG_LEVEL") or os.environ.get("LOG_LEVEL")
    if flags["debug"]:
        level_name = "debug"
    if level_name:
        configure_logging(level_from_name(level_name))

    opts = load_options_from_supervisor()  # Загружаем опции из /data/options.json

    if not opts:
        log.error("No options found. This add-on must run under Home Assistant Supervisor.")
        sys.exit(1)

    if not level_name:
        configure_logging(level_from_name(opts.get("log_level", "info")))

    cfg = build_cfg_from_options(opts)
    cfg["parameters"] = build_parameters(opts)

    if cfg["meter"].get("read_mode") == READ_MODE_ARRAY:
        # Суточный архив лежит в массиве кольцом, и порядок записей по данным не
        # восстанавливается — в этом режиме он не опрашивается и не объявляется
        # в discovery, чтобы не публиковать неверные сутки.
        daily = sorted({p["cmd"] for p in cfg["parameters"] if p["cmd"] == "EADPE"})
        cfg["parameters"] = [p for p in cfg["parameters"] if p["cmd"] != "EADPE"]
        if daily:
            log.info("Режим array: суточный архив (%s) исключён из опроса и "
                     "discovery — порядок записей в кольцевом буфере не "
                     "восстанавливается. Суточное потребление считайте в Home "
                     "Assistant из накопленной энергии (utility_meter или "
                     "статистика).", ", ".join(daily))

    interval = opts.get("poll_interval", 60)
    slow_interval = opts.get("slow_poll_interval", 3600)

    log_effective_config(cfg, interval, slow_interval)

    if cfg["meter"].get("read_mode") == READ_MODE_ARRAY:
        log.info("Режим array: массив читается целиком каждый цикл, "
                 "slow_poll_interval не используется; poll_timeout (%s с) должен "
                 "быть не меньше времени вычитки массива",
                 cfg["poll"]["timeout"])

    # Режим диагностики: только проверка канала, без MQTT и опроса.
    if flags["probe"]:
        sys.exit(run_probe(cfg))

    # Снятие массива общего чтения: порядок параметров и сохранённый дамп.
    if flags["dump_array"]:
        sys.exit(run_dump_array(cfg))

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    client = build_mqtt_client(cfg)

    last_slow_poll = 0
    log.info(
        "Starting poll loop: read_mode=%s, fast_interval=%ss, slow_interval=%ss",
        cfg["meter"].get("read_mode"), interval, slow_interval,
    )

    while running:
        start = time.monotonic()
        mymeter = None
        try:
            # Проверяем, нужно ли читать "медленные" данные
            read_slow_data = (time.monotonic() - last_slow_poll) >= slow_interval
            log.debug("Цикл опроса: медленные данные %s",
                      "читаются" if read_slow_data else "пропускаются")
            mymeter = open_meter(cfg)
            values = read_all(cfg, mymeter, read_slow_data)
            publish_values(client, cfg, values)
            if read_slow_data:
                last_slow_poll = time.monotonic()
        except Exception as e:
            log.exception("Poll cycle failed: %s", e)
            try:
                client.publish(f"{cfg['mqtt']['topic_prefix']}/status",
                               "offline", retain=True)
            except Exception:
                pass
        finally:
            if mymeter is not None:
                close_meter(cfg, mymeter)

        elapsed = time.monotonic() - start
        sleep_time = max(1, interval - elapsed)
        log.debug("Цикл опроса занял %.3f с, пауза %.1f с", elapsed, sleep_time)
        # спим короткими порциями, чтобы быстро реагировать на SIGTERM
        for _ in range(int(sleep_time)):
            if not running:
                break
            time.sleep(1)

    client.publish(f"{cfg['mqtt']['topic_prefix']}/status", "offline", retain=True)
    client.loop_stop()
    client.disconnect()
    log.info("Bye")


if __name__ == "__main__":
    main()
