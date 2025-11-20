install:
	python -m venv .venv && .venv/Scripts/activate && pip install -r requirements.txt

run:
	python src/main.py

test:
	pytest

lint:
	flake8 src/

format:
	black src/
