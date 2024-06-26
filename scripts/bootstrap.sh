#!/usr/bin/env bash
set -eo pipefail

cd /srv/root

if [ -z "$APP_ENV" ]; then
  echo "Please set APP_ENV"
  exit 1
fi

if [[ $PULL_SECRETS_FROM_VAULT -eq 1 ]]; then
  akatsuki vault get new-cron $APP_ENV -o .env
  source .env
fi

# await database availability
/scripts/await-service.sh $DB_HOST $DB_PORT $SERVICE_READINESS_TIMEOUT

exec python3 main.py
