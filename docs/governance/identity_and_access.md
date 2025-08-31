# Identity & Access Management (IAM)

## Overview
Databricks user and group management is integrated with **Azure Active Directory (AAD)**.  
Groups are created in AAD and automatically synced to Databricks using SCIM provisioning.  

## Steps

### 1. Create Groups in Azure AD
- `data_engineers` → Developers managing ingestion and transformations  
- `bi_users` → Reporting/analyst users who access curated data  
- `executives` → Business users with access to executive dashboards  

### 2. Enable SCIM Provisioning
1. In **Databricks Admin Console** → Generate a **SCIM token**.  
2. In **Azure AD**:
   - Go to **Enterprise Applications → + New application**.  
   - Search for **Databricks SCIM** and add it.  
   - In **Provisioning → Admin Credentials**, paste the SCIM token.  
   - Turn on **Automatic Provisioning**.  

### 3. Assign Users to Groups in AAD
- Add members to the above groups in Azure AD.  
- Sync runs automatically (~40 mins) or can be forced manually.  

### 4. Validate in Databricks
- In Databricks → **Admin Console → Identity and Access → Groups**  
- Ensure `data_engineers`, `bi_users`, `executives` exist.  

## Notes
- Always manage users **only in Azure AD**, never directly in Databricks.  
- Removing a user from AAD group removes their access in Databricks.  
