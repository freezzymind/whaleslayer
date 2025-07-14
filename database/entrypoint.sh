#!/bin/bash

# Подстановка переменных в шаблон init.template.sql → init.generated.sql
envsubst < ./init.template.sql > ./init.generated.sql

# Запуск docker-compose с уже подготовленным init.sql
docker-compose up -d