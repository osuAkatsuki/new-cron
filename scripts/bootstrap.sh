#!/usr/bin/env bash
set -eo pipefail

cd /srv/root

if [ -z "$APP_ENV" ]; then
  echo "Please set APP_ENV"
  exit 1
fi

if [[ $PULL_SECRETS_FROM_VAULT -eq 1 ]]; then
  pip install -i $PYPI_INDEX_URL akatsuki-cli
  # TODO: revert to $APP_ENV
  akatsuki vault get new-cron production-k8s -o .env
  source .env
  ls .env
  pwd
  # cat .env
fi

# await database availability
/scripts/await-service.sh $DB_HOST $DB_PORT $SERVICE_READINESS_TIMEOUT

exec python3 main.py
