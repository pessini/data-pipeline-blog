# Prefect Pipelines

```sh
prefect profile create lottery-project
prefect profile use lottery-project
prefect config set PREFECT_API_URL=http://localhost:17112/api
prefect deploy --prefect-file lottery/deployment.yaml  
```
