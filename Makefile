SHELL = := /bin/bash

PROJECT_NAME=
ENV = local

include .env

post: 
	@echo "Spinning up Postgres"
	docker-compose up -d