# Базовый образ Home Assistant для Python (актуальный тег)
FROM ghcr.io/home-assistant/base-python:3.14-alpine3.24

# Метки образа. Supervisor и GitHub Actions подставляют сюда реальные значения
# (BUILD_VERSION из config.yaml, BUILD_ARCH — текущая архитектура сборки);
# значения по умолчанию нужны только для ручной сборки через docker build.
# Без этих меток Supervisor считает образ «legacy».
ARG BUILD_VERSION="dev"
ARG BUILD_ARCH="aarch64|amd64"
LABEL \
    io.hass.version="${BUILD_VERSION}" \
    io.hass.type="app" \
    io.hass.arch="${BUILD_ARCH}"

# Устанавливаем git — нужен pip для установки из GitHub
RUN apk add --no-cache git

# Коммит библиотеки IEC61107 фиксируем: сборка должна быть воспроизводимой
# и не меняться от обновлений апстрима. При обновлении библиотеки укажите
# новый SHA (или переопределите при сборке: --build-arg IEC61107_COMMIT=...).
ARG IEC61107_COMMIT=473cbc9f3f8a11a184397e57b1be6465a01d1433

# paho-mqtt и pyserial фиксируем по версиям.
# pyyaml не нужен: опции читаются из /data/options.json (модуль json из stdlib).
RUN pip3 install --break-system-packages --no-cache-dir \
    paho-mqtt==2.1.0 \
    pyserial==3.5 \
    "git+https://github.com/Ko1dun/IEC61107_py.git@${IEC61107_COMMIT}"

COPY meter_bridge.py /
COPY run.sh /

# Приводим run.sh к LF: CRLF в этом файле ломает запуск аддона — шебанг
# превращается в «bashio<CR>», ядро ищет интерпретатор с именем «bashio\r»,
# и контейнер падает с «exec: fatal: unable to exec bashio».
RUN tr -d '\r' < /run.sh > /run.sh.lf \
    && mv /run.sh.lf /run.sh \
    && chmod a+x /run.sh

# Запускаем через /bin/sh, а не через шебанг: тогда запуск не зависит
# ни от окончаний строк в исходниках, ни от наличия bashio в базовом образе
# (в этом аддоне функции bashio не используются — опции читает Python).
CMD [ "/bin/sh", "/run.sh" ]
