#!/bin/bash

# Выход при ошибке (set -e), автоматический экспорт переменных (set -a)
set -ae

# Загрузка переменных окружения из .env
source /root/projects/whaleslayer/.env
set +a

# Подстановка переменных в шаблон SQL-файла (init.template.sql → init.generated.sql)
envsubst < ./init.template.sql > ./init.generated.sql

# Установка владельца файла и прав доступа, чтобы PostgreSQL (UID 999) мог прочитать init-файл
sudo chown 999:999 ./init.generated.sql
sudo chmod 644 ./init.generated.sql

# Запуск контейнера с PostgreSQL с использованием docker-compose
docker compose up -d
