#!/usr/bin/env make

build:
	docker build -t new-cron:latest .

run:
	docker run --network=host --env-file=.env -it new-cron:latest

run-bg:
	docker run --network=host --env-file=.env -d new-cron:latest