# dataos-pipeline

Run locally:
```
pipenv shell
pipenv install
pipenv run python -m pip freeze > requirements.txt
cp .env-example .env
# change DEPLOYMENT_NAME=my-local-deployment
chmod +x run-local.sh
prefect cloud login -k <prefect-api-key>
./run-local.sh
click buttons on https://app.prefect.cloud/account/734986ce-6773-48e8-82b8-3a3d0b504278/workspace/cb2e791d-2ca6-4697-9dce-41e4a329ad3f/deployments
```



prod:
```
pipenv install
pipenv run python -m pip freeze > requirements.txt
cp .env-example .env
# change DEPLOYMENT_NAME=my-prod-deployment
docker compose up -d
click buttons on https://app.prefect.cloud/account/734986ce-6773-48e8-82b8-3a3d0b504278/workspace/cb2e791d-2ca6-4697-9dce-41e4a329ad3f/deployments
```
