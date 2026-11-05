# Integration Guide — Data Lineage with OpenLineage

## 1. Overview

This guide explains how to configure and execute the Data Lineage with OpenLineage project.

The project integrates:

- Apache Spark for batch processing.
- Apache Kafka for event ingestion.
- Apache Flink for streaming analytics.
- OpenLineage for tracking data dependencies.
- Marquez for lineage visualization.
- Pytest for automated testing.

All services are orchestrated using Docker Compose.

## 2. Prerequisites

Install the following software:

- Docker Desktop
- Python 3.11 or later
- Git
- GitHub account

Ensure Docker Desktop is running before starting the project.

## 3. Clone the Repository

Clone the repository:

```bash
git clone https://github.com/nishithasathya/data-lineage-openlineage.git