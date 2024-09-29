#!/bin/bash

git clone -b $GIT_BRANCH --depth 1 $GIT_REPO git_repo

cd git_repo/app/dbt
dbt build

cd ..
python app.py

echo 'Container script done!'