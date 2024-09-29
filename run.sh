#!/bin/bash

git config --global --add safe.directory /volume_referral/.git
git clone -b $GIT_BRANCH --depth 1 $GIT_REPO git_repo

cd git_repo/app/dbt
dbt deps
dbt build

cd ..
python app.py

echo 'Container script done!'

# docker run --rm -it -v ./volume:/volume -v /home/eric/mm_referral_program:/volume_referral --env-file .env us-central1-docker.pkg.dev/bbg-platform/data-model/mm_referral_program