# Data Pipeline with Docker Compose

This project runs a complete data pipeline using Docker Compose with the following services:
- **Prefect**: Workflow orchestration platform
- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Database for Prefect metadata
- **Streamlit**: Dashboard for data visualization

## Quick Start

1. **Clone the repository and navigate to the project root**

2. **Review and customize the environment variables**
   ```bash
   cp .env.example .env
   # Edit .env file with your preferred credentials
   ```

3. **Start all services**
   ```bash
   docker-compose up -d
   ```

4. **Access the services**
   - **Streamlit Dashboard**: http://localhost:17113
   - **Prefect UI**: http://localhost:17112
   - **MinIO Console**: http://localhost:17111
   - **MinIO API**: http://localhost:17110

## Service Details

### Prefect (Workflow Orchestration)
- **Web UI**: http://localhost:17112
- Manages and monitors data pipeline workflows
- Uses PostgreSQL for metadata storage
- Includes a worker for executing flows

### MinIO (Object Storage)
- **Console**: http://localhost:17111
- **API**: http://localhost:17110
- S3-compatible object storage for data files
- Default credentials: `minioadmin` / `minioadmin_password_change_me`

### Streamlit Dashboard
- **URL**: http://localhost:17113
- Displays lottery data and analytics
- Connects to MinIO for data access

### PostgreSQL
- Used by Prefect for metadata storage
- Not exposed externally (internal service only)

## Environment Variables

The `.env` file contains all configuration:

```bash
# PostgreSQL Configuration for Prefect
POSTGRES_USER=prefect
POSTGRES_PASSWORD=prefect_password_change_me
POSTGRES_DB=prefect

# Prefect Configuration
PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://prefect:prefect_password_change_me@prefect-postgres:5432/prefect
PREFECT_API_URL=http://prefect-server:4200/api
PREFECT_DOCKER_VERSION=0.6.4

# MinIO Configuration
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin_password_change_me
MINIO_ENDPOINT=minio:9000
MINIO_BUCKET=lottery
MINIO_REGION=sa-east-1

# Streamlit Dashboard Configuration
S3_ENDPOINT_URL=http://minio:9000
S3_ACCESS_KEY_ID=minioadmin
S3_SECRET_ACCESS_KEY=minioadmin_password_change_me
S3_BUCKET_NAME=lottery
S3_REGION=sa-east-1
DUCKDB_FILE_PATH=lottery_results.duckdb
```

## Common Commands

### Start all services
```bash
docker-compose up -d
```

### Stop all services
```bash
docker-compose down
```

### View logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f lottery-dashboard
docker-compose logs -f prefect-server
docker-compose logs -f minio
```

### Rebuild and restart services
```bash
docker-compose up -d --build
```

### Clean up (remove volumes and data)
```bash
docker-compose down -v
```

## Data Pipeline Workflow

1. **Data Ingestion**: Prefect flows fetch lottery data from APIs
2. **Data Storage**: Raw data is stored in MinIO buckets
3. **Data Processing**: Prefect processes and transforms data
4. **Database Creation**: Processed data is stored in DuckDB files
5. **Visualization**: Streamlit dashboard displays the processed data

## Troubleshooting

### Service Health Checks
Check if services are healthy:
```bash
docker-compose ps
```

### MinIO Bucket Setup
If the lottery bucket doesn't exist, create it via the MinIO console at http://localhost:17111

### Prefect Worker Pool
After starting Prefect, you may need to create a worker pool in the UI at http://localhost:17112

### Port Conflicts
If you have port conflicts, modify the port mappings in `docker-compose.yml`:
```yaml
ports:
  - "17114:8501"  # Change 17113 to 17114 for Streamlit
  - "17115:4200"  # Change 17112 to 17115 for Prefect
```

## Development

### Building Images Locally
The compose file builds images from local Dockerfiles:
- `lottery-dashboard`: Built from `./dashboard/Dockerfile`
- `lottery-pipeline`: Built from `./prefect/lottery/Dockerfile`

### Volume Mounts
- MinIO data: Persistent volume `minio_data`
- PostgreSQL data: Persistent volume `prefect_db_data`
- Lottery code: Bind mount `./prefect/lottery:/opt/prefect/lottery`

## Security Notes

**Important**: Change default passwords in the `.env` file before deploying to production:
- `POSTGRES_PASSWORD`
- `MINIO_ROOT_PASSWORD`

For production deployments, consider:
- Using Docker secrets instead of environment variables
- Setting up proper network security
- Implementing backup strategies for volumes
- Using external databases and object storage
