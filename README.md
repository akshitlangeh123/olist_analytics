# Olist E-Commerce Analytics Platform

An end-to-end data engineering and analytics project built on the Brazilian Olist e-commerce dataset. Covers the full modern data stack — from raw CSV ingestion through a medallion architecture pipeline to a production-grade Power BI dashboard.

---

## Table of contents

- [Project overview](#project-overview)
- [Tech stack](#tech-stack)
- [Architecture](#architecture)
- [How it works](#how-it-works)
- [Data model](#data-model)
- [Dashboard](#dashboard)
- [Data quality findings](#data-quality-findings)
- [How to reproduce](#how-to-reproduce)
- [Project structure](#project-structure)
- [Skills demonstrated](#skills-demonstrated)

---

## Project overview

**Dataset**: Olist Brazilian E-Commerce — ~100,000 orders from 2016 to 2018 across 9 interrelated tables covering customers, sellers, products, payments, reviews, and geolocation.

**Business questions answered**:
- Where is revenue growing and where is it concentrated geographically?
- Which customer segments drive the most lifetime value?
- Are late deliveries damaging customer satisfaction — and which sellers are responsible?
- Which product categories are least profitable after freight costs?

---

## Tech stack

| Layer | Tool | Purpose |
|---|---|---|
| Storage | Databricks Serverless SQL Warehouse | Cloud data warehouse |
| Table format | Delta Lake | ACID transactions on cloud storage |
| Transformation | dbt Core 1.11 | SQL modelling, testing, documentation |
| Governance | Unity Catalog | Catalog → schema → table hierarchy |
| Visualisation | Power BI Desktop | Dashboards and DAX measures |
| Version control | Git + GitHub | Code versioning |

---

## Architecture

```
Raw CSVs (Kaggle)
      │
      ▼
  main.bronze        Raw Delta tables — 9 tables, as-is from source
      │
      ▼
  main.silver        dbt staging views — cleaned, typed, renamed
      │
      ▼
  main.gold          dbt mart tables — star schema, business-ready
      │
      ▼
  Power BI           3 dashboard pages connected via import mode
```

---

## How it works

### Bronze layer

The 9 Olist CSV files are uploaded directly into Databricks as Delta tables under `main.bronze`. Nothing is transformed at this layer — data is preserved exactly as received from the source. This means any upstream error can be traced back to the raw data at any point.

### Silver layer

dbt staging models sit on top of the bronze tables as SQL views. Each model corresponds to exactly one bronze table. At this layer the data is cleaned — columns are renamed to consistent snake_case, data types are cast correctly (timestamps, decimals, integers), null primary keys are filtered out, and duplicates are removed where necessary. No business logic lives here — it is purely a clean, typed representation of the source.

### Gold layer

dbt mart models join the staging views together and apply business logic. This is where metrics are calculated — delivery delay in days, whether an order was late, RFM scores for customer segmentation, seller tier classifications, and freight efficiency ratios. The output is a star schema stored as physical Delta tables, optimised for Power BI to query directly.

### Power BI

Power BI connects to the Databricks Serverless SQL Warehouse and imports the five gold tables. Relationships are drawn between the fact table and four dimension tables. DAX measures are written on top for time intelligence, rates, and aggregations. The report has three pages — Executive Summary, Customer Analytics, and Operations.

---

## Data model

The gold layer is structured as a star schema with one fact table at the centre and four surrounding dimension tables.

**fact_orders** sits at the order-item grain — one row per item within an order. It holds all foreign keys and numeric measures: price, freight value, payment value, days to deliver, delivery delay, late delivery flag, and review score.

**dim_customer** holds one row per unique customer with RFM segmentation. Customers are scored on Recency, Frequency, and Monetary value (each 1–5 using NTILE) and classified into six segments: Champions, Loyal, New customers, At risk, Lost, and Potential.

**dim_product** holds one row per product with category details (translated from Portuguese), sales performance metrics, and freight efficiency ratio.

**dim_seller** holds one row per seller with a tier classification (Bronze, Silver, Gold, Platinum based on total revenue), late delivery rate, and average review score.

**dim_date** is a pre-generated date spine covering the full dataset range. It enables time intelligence calculations in Power BI — including month-on-month growth and year-to-date revenue — and ensures months with zero orders still appear on charts rather than being skipped.

### Relationships

| Dimension | Key | Direction |
|---|---|---|
| dim_customer | customer_unique_id | One to many |
| dim_product | product_key | One to many |
| dim_seller | seller_key | One to many |
| dim_date | date_key | One to many |

---

## Dashboard

### Executive summary
Key metrics at a glance — total revenue, total orders, average order value, late delivery rate, and average review score. Includes a monthly revenue and order volume trend, revenue by order status, revenue concentration by Brazilian state, and a payment method breakdown.
<img width="2268" height="1329" alt="Screenshot 2026-04-20 021046" src="https://github.com/user-attachments/assets/6a1cdc2b-bd7f-441d-84ba-840a7402af41" />


### Customer analytics
Customer segmentation and retention analysis. Includes RFM segment distribution, revenue contribution by segment, orders per month, top 10 customers by lifetime spend, and review score distribution.
<img width="2266" height="1332" alt="Screenshot 2026-04-20 021107" src="https://github.com/user-attachments/assets/3658e3b9-3ede-458f-a895-21435cf3f5ff" />


### Operations
Delivery and seller performance. Includes late delivery rate by state, seller delivery speed vs customer satisfaction scatter, on-time vs late deliveries over time, delivery delay distribution, seller performance table, and average review score by seller tier.
<img width="2255" height="1334" alt="Screenshot 2026-04-20 021121" src="https://github.com/user-attachments/assets/7403e119-9ead-444a-815e-8079ec00cf87" />


---

## Data quality findings

These issues were discovered during dbt testing and are documented here rather than silently handled.

| Finding | Rows affected | Decision |
|---|---|---|
| Null review scores | 4,937 (5.0%) | Filtered in staging — customers opened the form but did not submit |
| Null order IDs on reviews | 2,236 (2.3%) | Filtered in staging — orphaned records with no linked order |
| Duplicate reviews per order | 547 orders | Deduplicated using ROW_NUMBER — kept the most recent review |
| Customers with multiple addresses | 2,997 customers | Deduplicated using ROW_NUMBER — kept the most recent address |
| One order with no payment record | 1 row | Downgraded to warning — genuine source data gap |
| Invalid review score outside 1–5 | 1 row | Filtered in staging |

---

## How to reproduce

### Prerequisites

- Python 3.11 or higher
- Databricks account with a Serverless SQL Warehouse
- Power BI Desktop
- Git

### Steps

1. Clone this repository
2. Create a Python virtual environment and install `dbt-databricks`
3. Configure your Databricks credentials in `~/.dbt/profiles.yml` using the template in `profiles.yml.example`
4. Download the Olist dataset from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and upload all 9 CSVs to `main.bronze` via the Databricks UI
5. Run `dbt debug` to verify the connection
6. Run staging models, then fact_orders, then the dimension models in order
7. Open `olist_analytics.pbix` in Power BI Desktop and update the data source credentials
8. Refresh the report

---

## Project structure

```
olist-analytics/
├── olist_analytics/
│   ├── models/
│   │   ├── staging/          ← Silver layer — 9 staging models + sources + schema tests
│   │   └── marts/            ← Gold layer — fact_orders + 4 dimensions + schema tests
│   ├── macros/               ← Custom schema name macro
│   ├── dbt_project.yml
│   └── profiles.yml.example
├── screenshots/              ← Dashboard screenshots for README
├── demo/                     ← Pipeline update demo recording
├── olist_analytics.pbix      ← Power BI report
└── README.md
```

---

## Skills demonstrated

- **Databricks** — Serverless SQL Warehouse, Unity Catalog, Delta Lake
- **dbt Core** — Staging models, mart models, schema tests, custom macros, lineage graph
- **SQL** — Window functions, CTEs, aggregations, date arithmetic, deduplication patterns
- **Data modelling** — Medallion architecture, star schema, fact and dimension table design, grain definition
- **Power BI** — Star schema relationships, DAX measures, time intelligence, Power Query transformations
- **Data quality** — Test-driven development with dbt, documented findings, severity classification
- **Version control** — Git, GitHub, structured commit history
