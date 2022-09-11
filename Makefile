.PHONY: all venv clean
.DEFAULT_GOAL := help

help:
	@echo "Available commands are:"
	@echo "venv"
	@echo "lint"

venv:
	test -d venv || python3 -m virtualenv venv
	(\
	source venv/bin/activate;\
	pip install -r requirements.txt;\
	)

lint:
	(\
	source venv/bin/activate;\
	python3 -m black ./app;\
	python3 -m isort ./app;\
	python3 -m flake8 ./app --ignore=E501,W503;\
	)

pg_up:
	docker run --name local_postgres -e POSTGRES_PASSWORD=supersecret -e POSTGRES_HOST_AUTH_METHOD=md5 -d -p 5433:5432 postgres
	docker exec -ti -u postgres local_postgres bash -c "echo 'password_encryption=md5' >> /var/lib/postgresql/data/postgresql.conf"
	docker restart local_postgres
	# docker exec -it local_postgres /bin/bash
	# psql -h 127.0.0.1 -p 5433 -U postgres

pg_down:
	docker stop local_postgres && docker rm local_postgres