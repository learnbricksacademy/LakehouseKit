# ADR 0001: Use Databricks for Data Engineering

**Date:** 2025-08-31  
**Status:** Accepted  

## Context
We need a unified data engineering platform that can handle ingestion, transformation, governance, and ML workloads.  

## Decision
We will use **Azure Databricks** as the core platform for this project due to its scalability, managed Delta Lake support, and Unity Catalog integration.  

## Consequences
- Positive: Strong integration with Azure, built-in governance.  
- Negative: Vendor lock-in to Databricks runtime.  
