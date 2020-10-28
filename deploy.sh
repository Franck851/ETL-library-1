#!/bin/bash

python inc_version_minor.py

rm -rf C:/Users/PARJ010/Documents/doc/html/*
rm -rf C:/Users/PARJ010/Documents/doc/latex/*
cp -R C:/Users/PARJ010/Documents/ETL_namespace/doc/* C:/Users/PARJ010/Documents/doc

( cd C:/Users/PARJ010/Documents/ETL_namespace/ETL && doxygen )
( cd C:/Users/PARJ010/Documents/doc && git config user.name "charlesdarkwind" && git add . && git commit -m "Auto-doc update" && git push origin master )

python setup.py install --user
Python setup.py sdist bdist_wheel
python -m twine upload dist/* --verbose --skip-existing