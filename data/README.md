## 🧠 Overview

The data layer is designed to support scalable, reliable, and analytics-ready data processing using Delta Lake and PySpark.

- Raw: Source ingestion (NDJSON files)
- Bronze: Raw structured data from Kafka
- Silver: Cleaned and enriched datasets
- Gold: Aggregated, BI-ready data models

# 🧊 Data Layer

This directory represents the storage and transformation layers of the data pipeline, following the Medallion Architecture:

**Raw → Bronze → Silver → Gold**

## 📂 Directory Structure

```text
/data/
├── raw/
├── bronze/
├── silver/
├── gold/

## 👨‍💻 Author

Odis Richardson
