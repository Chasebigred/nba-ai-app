# NBA AI App

A full-stack NBA analytics application that ingests official NBA data, stores it in a PostgreSQL data warehouse, and serves fast, query-optimized endpoints to a modern React frontend. The system is designed with production-style architecture and cloud deployment on AWS in mind.

---

## Overview

The NBA AI App separates data ingestion from data consumption to ensure reliability, performance, and scalability.

- External NBA data is ingested via scheduled ETL jobs  
- All frontend reads come exclusively from a PostgreSQL warehouse  
- No live third-party API calls occur on user-facing requests  
- Leaderboards and aggregates are computed from stored data  

This mirrors real-world analytics systems where read performance and API stability are critical.

---

## Features

- Player leaderboards (points, rebounds, assists, FG%, 3PT%)
- Player search with recent game breakdowns
- Precomputed warehouse-backed analytics
- FastAPI REST API
- PostgreSQL warehouse with Alembic migrations
- ETL pipeline using `nba_api`
- React + Vite frontend with Tailwind UI components
- Cloud-ready configuration for AWS deployment

---

## Tech Stack

### Backend
- FastAPI
- Python
- SQLAlchemy
- PostgreSQL
- Alembic
- nba_api

### Frontend
- React
- TypeScript
- Vite
- Tailwind CSS
- shadcn/ui

### Infrastructure
- AWS (RDS, EC@)
- Docker (local database only, not used in production)
- Environment-based configuration (no secrets in repository)

---

## Architecture

### Read Path
**Frontend → FastAPI → PostgreSQL Warehouse**

- All user-facing endpoints read from the warehouse
- Queries are optimized for fast leaderboard and player lookups
- No dependency on external APIs during reads

### Write Path (ETL)
**NBA API → ETL Jobs → PostgreSQL Warehouse**

- ETL jobs pull data from `stats.nba.com`
- Data is normalized and upserted using SQLAlchemy
- Standings and player stats are refreshed independently of frontend traffic

This design ensures consistent performance even if external data sources are unavailable.

---

## Repository Structure
nba-ai-app/
├── backend/
│ ├── main.py # FastAPI application
│ ├── db.py # Database configuration
│ ├── models.py # ORM models
│ ├── requirements.txt
│ ├── alembic/ # Database migrations
│ └── etl/ # Data ingestion jobs
│
├── frontend/
│ ├── src/ # React application
│ ├── public/
│ ├── package.json
│ └── vite.config.ts
│
├── docker-compose.yml # Local development database only
└── README.md


---

## Deployment

This application is deployed on AWS with a cost-conscious setup suitable for a portfolio project.

- Frontend: AWS Amplify Hosting with CI/CD from GitHub
- Backend: FastAPI running on a single Amazon EC2 instance (Docker or systemd-managed Uvicorn)
- Database: Amazon RDS for PostgreSQL
- ETL: Scheduled ETL jobs run on the backend host (cron/systemd timer) to ingest NBA data and upsert into the warehouse
- Configuration/Secrets: Environment variables (optionally stored in AWS Systems Manager Parameter Store / Secrets Manager)

This architecture keeps user-facing requests fast and stable by serving reads from the PostgreSQL warehouse while isolating ingestion work to scheduled ETL runs.


---

## Motivation

This project was built to demonstrate:

- Production-style backend design
- Data warehousing and ETL pipelines
- Clean API boundaries
- Full-stack development skills
- Cloud-first architecture decisions
- Polished React frontend

The focus is on system design and correctness rather than a minimal demo.

---

## Author

**Chase Brown**  
Computer Science (Software Engineering)  
AWS-focused Full-Stack Developer  

GitHub: https://github.com/Chasebigred
