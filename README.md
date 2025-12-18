# Shopzada Data Warehouse (ELT Pipeline)

## Overview

This project consolidates heterogeneous departmental datasets (CSV, XLSX, JSON, Parquet, Pickle, HTML) into a single analytical Data Warehouse implemented in PostgreSQL. The solution implements a modern ELT pipeline to provide a unified analytical view of the business’s core processes, including **Orders** and **Campaign Performance**.

The pipeline consists of:
- **Extract/Load (Ingestion):** Python scripts ingest raw departmental files into staging tables.
- **Transform:** Dimensional modeling (Kimball) produces a star schema of conformed dimensions and fact tables.
- **Orchestration:** A workflow engine (Windmill) coordinates ingestion and transformations.
- **Consumption:** BI/reporting via Metabase.

## Project Structure

Here is an overview of the project's folder structure and where key components are located:

### `datasets/`
Contains the raw data files organized by department.
- **Business Department/**: Excel files (`.xlsx`).
- **Customer Management Department/**: User data in Pickle, JSON, and CSV formats.
- **Enterprise Department/**: Merchant and order data in HTML, Parquet, and CSV formats.
- **Marketing Department/**: Campaign data in CSV format.
- **Operations Department/**: Order and line item data in various formats (Parquet, Pickle, CSV, Excel, JSON, HTML).
- **Test Files/**: Dirty and late data files used for testing the robustness of the pipeline.

### `scripts/`
Contains the Python scripts responsible for the ELT process.
- **`ingestions/`**: Scripts that extract raw data from `datasets/` and load it into the staging area of the data warehouse.
- **`tranforms/`**: Scripts that transform the staged data into the final Dimensional Model (Star Schema), creating Dimension (`DIM_*`) and Fact (`FACT_*`) tables.
- **`testcased/`**: Scripts specifically designed for ingesting test cases (dirty or late data).
- **`clean/`**: Scripts used for testing and validating data cleaning and transformation logic.

### `docs/`
Project documentation.
- **`technical_report.md`**: Detailed technical report covering architecture, scope, and methodology.
- **`architecture.md`**: High-level architecture documentation.
- **`data_dictionary.md`**: Detailed descriptions of the tables and columns in the data warehouse.
- **`data_lineage.md`**: Traceability of data from source to destination.
- **`star_schema.mermaid`**: Visual representation of the data model.

### `infra/`
Infrastructure-as-Code and configuration files.
- Includes `docker-compose.yml` for setting up the local development environment (PostgreSQL, etc.).

### `workflows/`
Configuration for the orchestration engine.
- Defines the Windmill workflows that coordinates the execution of ingestion and transformation scripts.

### `dashboard/`
- Contains artifacts related to the BI Dashboard (e.g., Shopzada Dashboard PDF).

### `video/`
- Contains pre-recorded final presentation videos.
