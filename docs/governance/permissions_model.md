# Permissions & Governance Model

## Layers
We use **Unity Catalog** to enforce governance:

- **Catalogs** → Represent projects (e.g., `cfp`, `fraud`).  
- **Schemas** → Data zones (`raw`, `harmonize`, `refined`).  
- **Tables & Views** → Actual datasets and curated logic.  

## Group Access
| Group          | Access Type             | Scope              |
|----------------|-------------------------|--------------------|
| data_engineers | ALL PRIVILEGES          | raw, harmonize     |
| bi_users       | SELECT only             | refined            |
| executives     | SELECT on curated views | refined (views)    |

## Example Grants
```sql
GRANT ALL PRIVILEGES ON SCHEMA cfp.raw TO `data_engineers`;
GRANT SELECT ON SCHEMA cfp.refined TO `bi_users`;
GRANT SELECT ON VIEW cfp.refined.claims_summary TO `executives`;
