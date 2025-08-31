# Governance Policies

## Access Control
- Use Unity Catalog to assign table/volume permissions.  
- Restrict write access to raw/landing zones.  
- Read-only access for BI users on refined zone.  

## Data Quality
- Great Expectations checks stored in `configs/harmonize/dq_expectations.yaml`.  
- Fail pipelines if critical DQ tests fail.  

## Auditing
- Log all pipeline runs in Delta tables (`system.audit_logs`).  
- Alerts configured in `alerts/alerts.json`.  

## Lineage
- Use Unity Catalog’s lineage feature to track dataset dependencies.  
