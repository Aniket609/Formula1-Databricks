# Formula1 Databricks ETL Project

A Databricks project for processing Formula 1 racing data. This project demonstrates a complete data pipeline implementation using Azure Databricks, Delta Lake, and PySpark.

## Project Overview

This project implements a three-layer ETL architecture to process Formula 1 racing data:
- **Ingestion Layer**: Loads raw CSV data from Azure Data Lake Storage
- **Transformation Layer**: Processes and enriches the data
- **Presentation Layer**: Creates analytics-ready tables and visualizations

## Architecture

The project follows a medallion architecture pattern:
```
Raw Data (CSV) → Processed (Delta Lake) → Presentation (Analytics Tables)
```

## Project Structure

```
Formula1-Databricks/
├── ingestion/              # Data ingestion notebooks
│   ├── circuits_ingest.py
│   ├── constructors_ingest.py
│   ├── drivers_ingest.py
│   ├── laptimes_ingest.py
│   ├── pitstops_ingest.py
│   ├── qualifying_ingest.py
│   ├── races_ingest.py
│   └── results_ingest.py
├── transformation/         # Data transformation notebooks
│   ├── constructor_standings.py
│   ├── driver_standings.py
│   └── race_results.py
├── presentation/           # Analytics and visualization queries
│   ├── dominant_drivers.sql
│   ├── dominant_drivers_visualization.sql
│   ├── dominant_teams.sql
│   └── dominant_teams_visualization.sql
├── Workflows/             # Databricks workflow definitions
│   ├── Ingest All.json/yaml
│   ├── Transform All.json/yaml
│   ├── Present All.json/yaml
│   └── Run Formula1 Project.json/yaml
├── functions.py           # Utility functions (mounts, optimization)
└── cluster.json          # Cluster configuration
```

## Prerequisites

- Azure Databricks workspace
- Azure Storage Account with Data Lake Gen2
- Azure Key Vault for secret management
- Service Principal with appropriate permissions
- Raw Formula 1 data stored in CSV format in the raw container

## Setup Instructions

### 1. Configure Azure Key Vault Secrets

Create a secret scope `formula1_scope` in Databricks and store the following secrets:
- `formula1-application-id`: Service Principal Application ID
- `formula1-directory-id`: Azure AD Tenant/Directory ID
- `formula1-client-secret`: Service Principal Client Secret

### 2. Create Storage Containers

Ensure the following containers exist in your Azure Storage Account:
- `raw`: Contains source CSV files
- `processed`: Stores processed Delta Lake tables
- `presentation`: Stores final analytics tables

### 3. Run Setup Functions

Execute `functions.py` to:
- Create mount points for storage containers
- Set up database configurations

### 4. Execute Workflows

Run the Databricks workflows in order:
1. **Ingest All**: Loads all raw data files into processed layer
2. **Transform All**: Transforms data and creates enriched tables
3. **Present All**: Generates analytics and visualization tables

Or use the **Run Formula1 Project** workflow which orchestrates all three steps sequentially.

## Data Sources

The project processes the following Formula 1 data entities:
- **Circuits**: Race circuit information
- **Constructors**: Team/constructor details
- **Drivers**: Driver information
- **Races**: Race details and schedules
- **Results**: Race results and standings
- **Qualifying**: Qualifying session results
- **Lap Times**: Detailed lap timing data
- **Pit Stops**: Pit stop information

## Key Features

- **Delta Lake Integration**: Uses Delta tables for ACID transactions and time travel
- **Incremental Processing**: Implements merge operations for upsert logic
- **Data Quality**: Schema enforcement during ingestion
- **Workflow Orchestration**: Automated pipeline execution
- **Analytics Ready**: Pre-built queries for common Formula 1 analytics

## Workflow Parameters

The main workflow accepts the following parameters:
- `raw_path`: Path to raw data (default: `/mnt/aniketformula1dl/raw`)
- `processed_path`: Path to processed data (default: `/mnt/aniketformula1dl/processed`)
- `presentation_path`: Path to presentation data (default: `/mnt/aniketformula1dl/presentation`)

## Analytics Queries

The presentation layer includes queries for:
- **Dominant Drivers**: Identifies top-performing drivers based on calculated points
- **Dominant Teams**: Analyzes team performance over time
- **Time-based Analysis**: Filters by specific race year ranges

## Notes

- This project was created as part of learning Databricks ETL processes
- Uses Service Principal authentication for secure access to Azure Storage
- Implements Delta Lake merge operations for incremental data updates
- Includes database optimization with VACUUM and OPTIMIZE commands

