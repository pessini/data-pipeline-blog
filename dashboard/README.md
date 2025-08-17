# lottery-dashboard

Dashboard in Streamlit to display Brazilian lottery results and other stats.

## 📁 Project Structure

```
lottery-dashboard/
├── app/                          # Main application directory
│   ├── main.py                   # Streamlit app entry point
│   ├── modules/                  # Application modules
│   │   ├── __init__.py
│   │   └── data_service.py       # S3 and DuckDB service
│   └── __init__.py
├── requirements.txt              # Python dependencies
├── Dockerfile                    # Docker image configuration
├── docker-compose.yml           # Docker compose setup
├── .dockerignore                 # Docker ignore file
├── .env.example                  # Environment variables template
├── run_local.sh                  # Local development script
└── README.md                     # This file
```

## 🚀 Quick Start

### Option 1: Docker (Recommended)

1. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your MinIO credentials
   ```

2. **Run with Docker Compose:**
   ```bash
   docker-compose up --build
   ```

3. **Or run with Docker directly:**
   ```bash
   docker build -t lottery-dashboard .
   docker run -p 8501:8501 --env-file .env lottery-dashboard
   ```

### Option 2: Local Development

1. **Install dependencies:**
   ```bash
   uv pip install -r requirements.txt
   ```

2. **Set up environment:**
   ```bash
   cp .env.example .env
   # Edit .env with your MinIO credentials
   ```

3. **Run the application:**
   ```bash
   ./run_local.sh
   # Or manually: cd app && streamlit run main.py
   ```

## 🔧 Configuration

Configure your MinIO S3 instance by editing the `.env` file:

```env
S3_ENDPOINT_URL=http://s3.localhost
S3_ACCESS_KEY_ID=your_access_key
S3_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=lottery
S3_REGION=us-east-1
DUCKDB_FILE_PATH=lottery_results.duckdb
```

## 📊 Features

- Connect to MinIO S3 instance
- Query DuckDB lottery database
- Display latest lottery draws
- Interactive charts and visualizations
- Responsive web interface

## 🗄️ Database Schema

The app expects a DuckDB database with this schema:

```sql
CREATE TABLE IF NOT EXISTS lottery_results (
    game_name TEXT,
    draw_number INTEGER,
    draw_date DATE,
    file_path TEXT,
    winning_numbers JSON,
    prize_tiers JSON,
    PRIMARY KEY (game_name, draw_number)
)
```
