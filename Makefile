.PHONY: all install list lint release test version notebooks


PACKAGE := $(shell grep '^PACKAGE =' setup.py | cut -d "'" -f2)
VERSION := $(shell head -n 1 $(PACKAGE)/VERSION)


all: list

install:
	pip install --upgrade -e .[develop]

list:
	@grep '^\.PHONY' Makefile | cut -d' ' -f2- | tr ' ' '\n'

lint:
	pylama $(PACKAGE)

release:
	bash -c '[[ -z `git status -s` ]]'
	git tag -a -m release $(VERSION)
	git push --tags

test:
	tox

version:
	@echo $(VERSION)

notebooks:
	rm -rf .checkpoints beatles.csv && jupyter nbconvert --execute *.ipynb --to markdown --inplace
