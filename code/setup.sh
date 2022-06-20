#! /usr/bin/env bash
aws s3 cp s3://nome-do-bucket/us_census_bureau.log ./
aws s3 rm s3://nome-do-bucket/us_census_bureau.log
aws s3 cp s3://nome-do-bucket/us_census_bureau.db ./
aws s3 rm s3://nome-do-bucket/us_census_bureau.db
aws s3 cp etl.py s3://nome-do-bucket/
aws s3 cp parametros.json s3://nome-do-bucket/
aws s3 cp params.py s3://nome-do-bucket/
aws s3 cp start.sh s3://nome-do-bucket/
aws s3 cp finaliza.sh s3://nome-do-bucket/
aws s3 ls s3://nome-do-bucket/
