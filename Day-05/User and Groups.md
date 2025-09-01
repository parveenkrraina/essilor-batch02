# Managing Users and Groups in Azure Databricks with Unity Catalog

## Lab Objective

You will:

- Add a new user to Azure Databricks using Azure Active Directory.
- Create a group and add users to it.
- Assign Unity Catalog permissions to that group.
- Verify access control by querying data as different users.

**Estimated Time:** 45–60 minutes

---

## Lab Setup

- Azure portal access with permissions to manage Azure AD users and groups.
- Databricks workspace admin or Unity Catalog metastore admin rights.
- Unity Catalog must already be enabled.

---

## Step 1: Add a User in Azure Active Directory

1. Open [Azure Portal](https://portal.azure.com).
2. Navigate to **Azure Active Directory > Users > New user**.
3. Click **+ New user**.
4. Fill in user details (e.g., name: Jane Doe, username: janedoe@yourdomain.com).

**Assign User to Databricks Enterprise Application:**

1. In Azure AD, go to **Enterprise applications > Databricks**.
2. Click **Users and groups > Add user/group**.
3. Select Jane Doe and add.

---

## Step 2: Create a Group and Add Users

1. Go to **Azure AD > Groups > New group**.
2. Set **Group type:** Security.
3. **Group name:** `data_analysts`.
4. Assign owner and click **Create**.

**Add Members to the Group:**

1. Open the `data_analysts` group.
2. Click **Members > Add members**.
3. Add Jane Doe and any other relevant users.

**Assign Group to Databricks Application:**

1. Go to **Enterprise applications > Databricks > Users and groups > Add user/group**.
2. Select `data_analysts` and add.

---

## Step 3: Verify User and Group in Databricks

1. Log into Databricks Workspace (as admin).
2. Go to **Admin Console > Users**. Confirm Jane Doe is present.
3. Go to **Admin Console > Groups**. Confirm `data_analysts` is listed and contains Jane Doe.

---

## Step 4: Assign Unity Catalog Permissions to Group

Open Databricks SQL Editor or a notebook (as admin or metastore admin) and run:

**Note** Change catalog and schema names and table names as per your setup.

```sql
-- Grant catalog and schema usage
GRANT USAGE ON CATALOG uc_demo TO `data_analysts`;
GRANT USAGE ON SCHEMA uc_demo.raw TO `data_analysts`;

-- Grant SELECT on a table
GRANT SELECT ON TABLE uc_demo.raw.orders_v2 TO `data_analysts`;
```

---

## Step 5: Test Permissions as the New User

1. Log into Databricks as Jane Doe (use an incognito/private browser window).
2. Open Databricks SQL Editor or a notebook.
3. Run:

    ```sql
    SELECT * FROM sales_data_managed.raw.orders_v2;
    ```
    **Expected Result:** Jane Doe should be able to query the table.

4. Try to access a table with no granted access:

    ```sql
    SELECT * FROM some_other_catalog.schema.restricted_table;
    ```
    **Expected Result:** Permission denied.

---

## Step 6: (Optional) Revoke Access and Re-Test

As admin, run:

```sql
REVOKE SELECT ON TABLE sales_data_managed.raw.orders_v2 FROM `data_analysts`;
```

As Jane Doe, retry the query. You should now see a permissions error.

---

## Step 7: Clean Up (Optional)

- Remove test user from Azure AD or from Databricks.
- Delete the test group if not needed.
- Revoke test permissions.

---

## Validation Checklist

| Task                                              | Status |
|---------------------------------------------------|--------|
| User added in Azure AD                            | [ ]    |
| User assigned to Databricks enterprise app        | [ ]    |
| Group created in Azure AD                         | [ ]    |
| User added to group                               | [ ]    |
| Group assigned to Databricks enterprise app       | [ ]    |
| Group appears in Databricks                       | [ ]    |
| Permissions granted to group in Unity Catalog     | [ ]    |
| User successfully queries data                    | [ ]    |
| Access denied after revoke                        | [ ]    |