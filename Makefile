.PHONY: init test clean docs docs-test clean-docs-test check-sphinx

SHELL                     := /bin/bash
SPHINX                    := $(shell which sphinx-build)
GH_PAGES_SOURCES          := docs/Makefile docs/source
MASTER_HEAD_COMMIT        := $(shell git rev-parse master)
MASTER_HEAD_ABBREV_COMMIT := $(shell git rev-parse --short master)
#COMMIT_MSG_BODY    := $(shell git log master -1 --pretty=short --abbrev-commit)

init:
	pip install -r requirements-dev.txt

test:
	python setup.py test

clean:
	find ./cromulent -name "*.pyc" -exec rm {} \;

docs: check-sphinx
	git checkout -b gh-pages origin/gh-pages
	rm -rf build _sources _static
	git checkout master $(GH_PAGES_SOURCES)
	git reset HEAD
	cd docs && $(MAKE) --debug html && cd -
	mv -fv docs/build/html/* ./
	mv -fv docs/build/html/.nojekyll ./
	mv -fv docs/build/html/.buildinfo ./
	rm -rf $(GH_PAGES_SOURCES) docs/build docs
	git add *.html *.js _static _sources .nojekyll .buildinfo objects.inv
	git commit \
		-m "Generated gh-pages based on : $(MASTER_HEAD_ABBREV_COMMIT)" \
		-m "Full commit: $(MASTER_HEAD_COMMIT)"
	git push origin gh-pages
	git checkout master

docs-test:
	cd docs && $(MAKE) --debug html && cd -

clean-docs-test:
	cd docs && rm -rf build && cd -

check-sphinx:
ifndef SPHINX
	$(error "sphinx not installed:  run 'pip install Sphinx'")
endif

