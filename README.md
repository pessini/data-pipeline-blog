# 🎲 Lottery Pipeline Project

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://www.python.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://www.docker.com/)
[![Prefect](https://img.shields.io/badge/Prefect-3-lavender)](https://www.prefect.io/)
[![Streamlit](https://img.shields.io/badge/Streamlit-orange)](https://streamlit.io/)

This repository contains a **production-inspired end-to-end data pipeline** for official Brazilian lottery results. The project shows how to collect, store, transform, and visualize real-world data using modern Python tools and Docker.

## Project Overview

The pipeline processes lottery results from games like **Quina** and **Mega-Sena** through these steps:

1. **Fetch Data from API** – Pull historical lottery results as structured JSON.  
2. **Store Raw Data in MinIO** – Save each draw in an S3-compatible bucket.  
3. **Transform and Query with DuckDB** – Normalize fields and create queryable tables.  
4. **Visualize with Streamlit** – Interactive dashboard to explore results and statistics.

## Tech Stack

- **Prefect** – Orchestrates workflows with retries, scheduling, and monitoring.  
- **MinIO** – Local S3-compatible data lake for storing raw JSON data.  
- **DuckDB** – Columnar database optimized for fast querying and transformations.  
- **Streamlit** – Interactive, user-friendly dashboard.  
- **Docker** – Ensures reproducibility with isolated, connected containers.

## Quick Start

> ⚠️ **Note:** Please fork this repository before using it, so you can safely modify environment variables and experiment without affecting the original project.

All the services are set up in the `/docker` folder. To get the pipeline running:

1. Copy the environment example:
```bash
cp docker/.env.example docker/.env
```
2. Edit .env with your credentials if needed.
3. Start everything with
```bash
docker compose -f docker/docker-compose.yaml up -d
```
4. Access services:
    - MinIO API: http://localhost:17110
    - MinIO Console: http://localhost:17111
    - Prefect UI: http://localhost:17112
    - Streamlit Dashboard: http://localhost:17113

## Series Articles

For a detailed walkthrough, check out the three-part series:

- Part 1 – From API to Docker Compose Setup
- Part 2 – Writing Prefect Flows to Fetch, Store, and Transform Data
- Part 3 – Building and Deploying the Streamlit Dashboard