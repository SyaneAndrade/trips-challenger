local-windows:
	virtualenv -p python .venv
	.\.venv\Scripts\activate

local:
	virtualenv -p python .venv
	./.venv/Scripts/activate

install-requirements:
		pip install -r requirements.txt

build:
	docker-compose up airflow-init

up:
	docker-compose up -d

stop:
	docker-compose down
