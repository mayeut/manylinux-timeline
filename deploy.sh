#!/bin/bash

# Adapted from https://zellwk.com/blog/deploy-static-site/


# Prevents script from running if there are any errors
set -xeuo pipefail

# Gets commit hash as message
REV=$(git rev-parse HEAD)

git checkout --force gh-pages # Step 3

git rm -rf . # Step 4

git checkout gh-pages -- .gitignore # Step 5

cp -R build/* . && rm -rf build # Step 6

git add . # Step 7

git commit -m "Deploy $REV" # Step 8

git push origin gh-pages # Step 9

git checkout - # Step 10
