# Snowflake ELT Pipeline with dbt and Apache Airflow

An educational Data Engineering project demonstrating how to build, transform, test, and orchestrate an ELT pipeline using Snowflake, dbt, Apache Airflow, and Astronomer Cosmos.

The project focuses on SQL-based data transformations, analytical modeling, reusable macros, data quality testing, and workflow orchestration.

## Project Overview

The project transforms order and line-item data from Snowflake's TPC-H sample dataset into analytics-ready order-level models.

It demonstrates how to:

- Configure a Snowflake warehouse, database, schema, and role.
- Define source tables in dbt.
- Organize transformations into staging, intermediate, and analytical models.
- Join order and line-item data.
- Calculate discount amounts using reusable SQL macros.
- Aggregate order-level metrics.
- Validate data using dbt tests.
- Orchestrate the workflow with Apache Airflow and Astronomer Cosmos.

The repository also contains CSV seed files that can be managed by dbt.

## Architecture

```text
Snowflake Sample Data
SNOWFLAKE_SAMPLE_DATA.TPCH_SF1
              |
              v
       dbt Sources
       orders / lineitem
              |
              v
        Staging Models
  stg_tpch_orders
  stg_tpch_line_items
              |
              v
     int_order_items
  Join orders and line items
  Calculate discount amounts
              |
              v
 int_order_items_summary
 Aggregate sales and discounts
              |
              v
          fct_orders
  Order-level analytical model
              |
              v
      dbt Data Quality Tests

   Apache Airflow + Cosmos
      Workflow Orchestration
```

Snowflake provides the source data and executes the SQL transformations. dbt manages model dependencies and testing, while Apache Airflow orchestrates the workflow through Astronomer Cosmos.

## Tech Stack

| Technology | Purpose |
|---|---|
| Snowflake | Cloud data warehouse and SQL execution |
| SQL | Joins, aggregations, and data transformations |
| dbt | Data modeling, dependency management, macros, and tests |
| dbt-utils | Reusable SQL macros, including surrogate key generation |
| Apache Airflow | Workflow orchestration and scheduling |
| Astronomer Cosmos | Integration between dbt and Airflow |
| Python | Airflow DAG configuration |
| Docker | Containerized local development |
| Astro CLI | Local Airflow environment management |

## Snowflake Environment Setup

The project uses a dedicated Snowflake warehouse, database, schema, and role for dbt.

### Resources

| Resource | Name | Purpose |
|---|---|---|
| Virtual warehouse | `DBT_WH` | Compute resources for SQL queries |
| Database | `DBT_DB` | Stores project-owned objects |
| Schema | `DBT_SCHEMA` | Target schema for dbt models |
| Role | `DBT_ROLE` | Permissions for the dbt workload |

### 1. Create the warehouse, database, and role

Run the following commands in a Snowflake SQL worksheet using an administrator role:

```sql
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS dbt_wh
    WITH WAREHOUSE_SIZE = 'XSMALL';

CREATE DATABASE IF NOT EXISTS dbt_db;

CREATE ROLE IF NOT EXISTS dbt_role;
```

The `XSMALL` warehouse is intended for a small development workload.

### 2. Grant permissions

```sql
GRANT USAGE ON WAREHOUSE dbt_wh
    TO ROLE dbt_role;

GRANT ALL ON DATABASE dbt_db
    TO ROLE dbt_role;
```

Assign the role to your Snowflake user:

```sql
GRANT ROLE dbt_role TO USER <YOUR_SNOWFLAKE_USER>;
```

Replace the placeholder with your Snowflake username.

### 3. Create the target schema

```sql
USE ROLE dbt_role;

CREATE SCHEMA IF NOT EXISTS dbt_db.dbt_schema;

USE WAREHOUSE dbt_wh;
```

The `dbt_schema` schema is the target for the project's dbt models.

For production workloads, prefer least-privilege grants instead of broad database permissions.

### Source Data

The models use the following Snowflake sample data source:

- Database: `SNOWFLAKE_SAMPLE_DATA`
- Schema: `TPCH_SF1`
- Tables: `ORDERS` and `LINEITEM`

The source database is separate from the project's own `DBT_DB` database.

The role used by dbt must have the necessary permissions to read the sample tables and create the required objects in the target schema.

## dbt Data Transformation

The dbt project is located in:

```text
dags/dbt/data_pipeline/
```

The models are organized into staging and marts directories. The intermediate models are stored under `models/marts/` in the current project structure.

### 1. Source Definitions

The source definitions are maintained in `models/staging/tpch_sources.yml`.

```yaml
version: 2

sources:
  - name: tpch
    database: snowflake_sample_data
    schema: tpch_sf1
    tables:
      - name: orders
      - name: lineitem
```

This lets dbt reference existing Snowflake tables using `source()` rather than hardcoding their full database paths in each model.

The source definitions also include checks for order key uniqueness, non-null values, and relationships between line items and orders.

### 2. Staging Models

The staging layer standardizes source column names and prepares fields for downstream transformations.

**`stg_tpch_orders.sql`**

Selects order information, including:

- `order_key`
- `customer_key`
- `status_code`
- `total_price`
- `order_date`

**`stg_tpch_line_items.sql`**

Selects line-item data, including:

- `order_key`
- `part_key`
- `line_number`
- `quantity`
- `extended_price`
- `discount_percentage`
- `tax_rate`

For example:

```sql
SELECT
    o_orderkey AS order_key,
    o_custkey AS customer_key,
    o_orderstatus AS status_code,
    o_totalprice AS total_price,
    o_orderdate AS order_date
FROM {{ source('tpch', 'orders') }}
```

The `source()` function references a declared source table; it does not create that table.

### 3. Surrogate Keys

The line-item staging model generates a surrogate key using `dbt_utils.generate_surrogate_key()`:

```sql
{{
    dbt_utils.generate_surrogate_key([
        'l_orderkey',
        'l_linenumber'
    ])
}} AS order_item_key
```

The key is generated from the order key and line number. This creates an identifier for an individual order item based on the combination of those fields.

### 4. Intermediate Order Items

The model `int_order_items.sql` joins the staging orders and line-item models:

```sql
FROM {{ ref('stg_tpch_orders') }} AS orders
JOIN {{ ref('stg_tpch_line_items') }} AS line_item
    ON orders.order_key = line_item.order_key
```

It combines order attributes with individual line-item information.

The model also calculates the discount amount using the custom `discounted_amount` macro.

### 5. Reusable Discount Macro

The macro is defined in `macros/pricing.sql`:

```sql
{% macro discounted_amount(extended_price, discount_percentage, scale=2) %}
    (-1 * {{ extended_price }} * {{ discount_percentage }})::decimal(16, {{ scale }})
{% endmacro %}
```

The macro calculates a negative discount amount.

For example, if the extended price is `1000` and the discount rate is `0.05`, the result is:

```text
-1 × 1000 × 0.05 = -50.00
```

The negative value represents a deduction from gross sales. The macro is compiled into SQL and executed in Snowflake.

### 6. Order-Level Aggregation

The model `int_order_items_summary.sql` aggregates line-item amounts by order:

```sql
SELECT
    order_key,
    SUM(extended_price) AS gross_item_sales_amount,
    SUM(item_discount_amount) AS item_discount_amount
FROM {{ ref('int_order_items') }}
GROUP BY order_key
```

This produces one summary record per order, containing the sum of extended prices and the sum of discount amounts.

For example, if an order contains three items, their values are aggregated into a single order-level record.

### 7. Final Analytical Model

The `fct_orders.sql` model joins staged order information with the aggregated line-item summary:

```sql
SELECT
    orders.*,
    order_item_summary.gross_item_sales_amount,
    order_item_summary.item_discount_amount
FROM {{ ref('stg_tpch_orders') }} AS orders
JOIN {{ ref('int_order_items_summary') }} AS order_item_summary
    ON orders.order_key = order_item_summary.order_key
```

The resulting model contains the order attributes together with aggregated gross item sales and discount amounts.

This model can be used for reporting and further analytical transformations.

## Data Quality Testing

The project uses both generic and singular dbt tests.

### Generic Tests

The configuration in `models/marts/generic_tests.yaml` includes:

| Test | Purpose |
|---|---|
| `unique` | Checks that order keys are unique |
| `not_null` | Checks that order keys are not null |
| `relationships` | Checks that order keys exist in the referenced model |
| `accepted_values` | Checks that order status codes belong to the allowed set |

The configured status codes are `P`, `O`, and `F`.

The relationship test on `fct_orders.order_key` references `stg_tpch_orders.order_key`. Its severity is set to `warn`, so a violation produces a warning rather than a blocking error.

### Singular SQL Tests

Custom tests are stored in the project's `tests/` directory.

The project includes:

- `fct_orders_date_valid.sql`
- `fct_orders_dscount.sql`

A singular SQL test returns rows that violate the specified condition. No returned rows means the test passes.

For example, to flag positive discount amounts while allowing zero discounts:

```sql
SELECT *
FROM {{ ref('fct_orders') }}
WHERE item_discount_amount > 0
```

The correct condition depends on the business rule being tested.

### Running Tests

When dbt is configured to access the project and Snowflake, run:

```bash
dbt test
```

Inspect failed test queries and returned records to determine whether the problem comes from the data or from the test condition.

## Airflow Orchestration

The Airflow DAG is defined in:

```text
dags/dbt_dag.py
```

The project uses Astronomer Cosmos to integrate the dbt project with Airflow.

The DAG configuration includes:

| Setting | Value |
|---|---|
| DAG ID | `dbt_dag` |
| Schedule | `@daily` |
| Catchup | `False` |
| dbt project | `/usr/local/airflow/dags/dbt/data_pipeline` |
| Airflow connection ID | `snowflake_conn` |
| Target database | `dbt_db` |
| Target schema | `dbt_schema` |

Cosmos discovers the dbt project and integrates its operations into Airflow, where task dependencies, status, and logs can be inspected.

The DAG uses `ProjectConfig`, `ProfileConfig`, `ExecutionConfig`, and `SnowflakeUserPasswordProfileMapping` to configure the project, profile, connection mapping, and dbt executable.

The configured executable path is:

```text
$AIRFLOW_HOME/dbt_venv/bin/dbt
```

Ensure that the executable and required dependencies are available inside the Airflow container at the configured path.

## Project Structure

```text
dbt-dag/
├── dags/
│   ├── dbt_dag.py
│   └── dbt/
│       └── data_pipeline/
│           ├── models/
│           │   ├── staging/
│           │   │   ├── stg_tpch_line_items.sql
│           │   │   ├── stg_tpch_orders.sql
│           │   │   └── tpch_sources.yml
│           │   └── marts/
│           │       ├── fct_orders.sql
│           │       ├── int_order_items.sql
│           │       ├── int_order_items_summary.sql
│           │       └── generic_tests.yaml
│           ├── macros/
│           │   └── pricing.sql
│           ├── seeds/
│           ├── tests/
│           │   ├── fct_orders_date_valid.sql
│           │   └── fct_orders_dscount.sql
│           ├── dbt_project.yml
│           ├── packages.yml
│           └── package-lock.yml
├── tests/
├── Dockerfile
├── requirements.txt
├── packages.txt
├── .dockerignore
├── .gitignore
└── README.md
```

Generated directories such as `target/`, `logs/`, and `dbt_packages/` are excluded from version control and can be regenerated when required.

## Prerequisites

To run the project locally, you need:

- Docker Desktop
- Astro CLI
- A Snowflake account
- A Snowflake warehouse, database, schema, and role with the necessary permissions
- An Airflow Snowflake connection
- The required dbt executable and dependencies
- The Python packages listed in `requirements.txt`

## Running Locally

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/elt-pipeline-snowflake-dbt-airflow.git
cd elt-pipeline-snowflake-dbt-airflow
```

Replace `YOUR_USERNAME` with your GitHub username.

### 2. Start Airflow

```bash
astro dev start
```

When the Airflow services are running, open:

http://localhost:8080

### 3. Configure Snowflake

Create an Airflow connection with the ID:

```text
snowflake_conn
```

Configure it with your own Snowflake account details and credentials. Ensure that the role and warehouse settings are compatible with the database, schema, and source tables referenced in the project.

The DAG expects the dbt executable at the path specified in `ExecutionConfig`. Adjust this path if your local Airflow environment uses a different installation.

### 4. Run the workflow

In the Airflow UI:

1. Locate the `dbt_dag` DAG.
2. Enable it if it is paused.
3. Trigger a run.
4. Inspect the task statuses and logs.
5. Review any failed seed, model, or test tasks.

### 5. Run dbt commands directly

When the dbt executable, profile, adapter, and Snowflake connection are configured in the execution environment, the following commands can be used to validate the project:

```bash
dbt deps
dbt seed
dbt run
dbt test
```

- `dbt deps` installs declared package dependencies.
- `dbt seed` loads CSV files into the target database.
- `dbt run` executes dbt models.
- `dbt test` executes configured data quality tests.

## Troubleshooting

### Seed table already exists

If a seed fails because the target table already exists, determine whether the table should be replaced with the CSV contents.

To refresh one seed:

```bash
dbt seed --select raw_orders --full-refresh
```

To refresh all seeds:

```bash
dbt seed --full-refresh
```

**Warning:** A full refresh replaces existing seed tables with data from the corresponding CSV files. Use it only when this is appropriate for your target environment.

### Airflow does not show the DAG

Check for import errors:

```bash
astro dev run dags list-import-errors
```

List registered DAGs:

```bash
astro dev run dags list
```

Verify the DAG configuration, Cosmos and Airflow compatibility, project path, and configured dbt executable path.

### dbt tests fail

Inspect the failed test SQL and identify the records returned by its query.

A failed singular test means the query returned rows that violate the test's condition. For generic tests, inspect the tested column, accepted values, null constraints, or relationships.

Ensure that the test conditions represent the intended business rules.

## Security

- Never commit `.env` files, passwords, access tokens, or private keys.
- Do not publish `airflow_settings.yaml` if it contains credentials or sensitive configuration.
- Use your own Snowflake credentials when running the project.
- Keep local configuration separate from the public repository.
- Use least-privilege permissions for production environments.

## Project Status

This is an educational Data Engineering portfolio project.

It demonstrates a Snowflake-based ELT workflow using dbt and Apache Airflow, including source definitions, SQL transformations, reusable macros, analytical models, data quality tests, and workflow orchestration.

Successful end-to-end execution depends on the configured Snowflake environment, permissions, dbt executable, seed state, and test conditions.
