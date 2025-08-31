# Setup Governance Notebook

## Purpose
The governance notebook (`notebooks/governance/apply_permissions.py`) automates the application of security and governance policies in Databricks Unity Catalog.

It ensures:
- Required **catalogs and schemas** exist.
- **Groups** (synced from Azure AD) are assigned correct privileges.
- **Permissions** are applied consistently from configuration files.

---

## Prerequisites
1. Azure AD groups (`data_engineers`, `bi_users`, `executives`) are created and synced into Databricks (see [identity_and_access.md](identity_and_access.md)).
2. Permissions are defined in:
