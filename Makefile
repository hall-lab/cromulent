.PHONY: init test

init:
	pip install -r requirements-dev.txt

test:
	python setup.py test
