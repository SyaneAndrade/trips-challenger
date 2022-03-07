local-windows:
	virtualenv -p python3 .venv
	.\.venv\Scripts\activate

install-requiresments:
		pip install -r requirements.txt

build:
	docker-compose up airflow-init

up:
	docker-compose up -d

stop:
	docker-compose down
