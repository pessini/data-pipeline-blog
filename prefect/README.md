# Prefect Pipelines


## Prefect Configuration

```sh
prefect profile create lottery-project
prefect profile use lottery-project
prefect config set PREFECT_API_URL=http://localhost:17112/api
```

### Prefect Variables
Create a variable using CLI
```sh
prefect variable set lottery_games '["lotofacil", "lotomania", "megasena", "quina", "loteca", "duplasena", "diadesorte", "supersete"]'
```

### Prefect Blocks
```sh
prefect block create minio-credentials
```
This will open a link in your browser to the Prefect UI, where you can fill in the details:
- Block Name: minio-credentials
- Endpoint URL: https://host.docker.internal:9000
- Minio Root User: minioadmin
- Minio Root Password: minioadmin_password_change_me

### Prefect Automation


### Prefect Deploy
```sh
prefect deploy --prefect-file lottery/deployment.yaml  
```
