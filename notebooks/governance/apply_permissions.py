
---

## 3. `docs/governance/setup_governance_notebook.md`
```markdown
# Setup Governance Notebook

## Purpose
Automates the application of:
- Unity Catalog setup (catalogs, schemas)
- Permissions (GRANTs)
- View creation from config

## Steps to Run
1. Open **Databricks Workspace**.  
2. Navigate to `notebooks/governance/apply_permissions.py`.  
3. Attach to the cluster.  
4. Run the notebook:
   - It reads `configs/governance/permissions.yaml`.  
   - Applies `GRANT` statements for groups (`data_engineers`, `bi_users`, `executives`).  
   - Logs all applied permissions.

## Config Structure
```yaml
permissions:
  - object: "cfp.refined.claims_summary"
    type: "VIEW"
    grants:
      - principal: "executives"
        privileges: ["SELECT"]
  - object: "cfp.raw"
    type: "SCHEMA"
    grants:
      - principal: "data_engineers"
        privileges: ["ALL PRIVILEGES"]
