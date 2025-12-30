# End-to-End SQL Data Warehouse Project (Data Engineering using SQL Server)

## 📌 Project Overview
This repository contains a full **end-to-end Data Engineering implementation** of a **SQL-based Data Warehouse** using **Microsoft SQL Server**.  
The project follows a **layered warehouse architecture (Bronze, Silver, Gold)** and demonstrates how raw operational data from **CRM and ERP systems** is transformed into **business-ready analytical models** using **pure SQL**.

The solution is designed to reflect **real enterprise data warehouse practices**, including structured ETL layers, stored procedures, data lineage, dimensional modeling, and documentation.

---

## 🏗️ High-Level Architecture
The overall system architecture follows a traditional enterprise data warehouse pattern.

**Key characteristics:**
- SQL Server as the warehouse engine
- Layered architecture for traceability and data quality
- Stored procedures for ETL orchestration
- Separation of raw, cleaned, and analytical data

📷 **Architecture Diagram**  
<img width="1544" height="777" alt="data_architecture" src="https://github.com/user-attachments/assets/d2847215-2e77-4370-950b-59900a72f01a" />


---

## 🧱 Data Warehouse Layers

### 🥉 Bronze Layer – Raw Data
**Purpose:**  
Ingest raw data from source systems without business transformations.

**Characteristics:**
- Tables mirror source system structures
- Batch processing using full load (truncate & insert)
- No transformations applied
- Acts as a recovery and audit layer

**Object Type:** Tables  
**Load Strategy:** Batch | Full Load | Truncate & Insert  

**Source Tables:**
- `crm_sales_details`
- `crm_cust_info`
- `crm_prd_info`
- `erp_cust_az12`
- `erp_loc_a101`
- `erp_px_cat_g1v2`

---

### 🥈 Silver Layer – Cleaned & Standardized Data
**Purpose:**  
Prepare high-quality, integrated data ready for analytical modeling.

**Transformations Applied:**
- Data cleansing (null handling, trimming, casting)
- Data standardization (naming, formats)
- Deduplication
- Normalization
- Derived columns
- Data enrichment across CRM and ERP

**Object Type:** Tables  
**Load Strategy:** Batch | Full Load | Truncate & Insert  

This layer resolves inconsistencies between CRM and ERP systems and creates a unified data model.

---

### 🥇 Gold Layer – Business-Ready Data
**Purpose:**  
Expose analytics-ready datasets for reporting and analysis.

**Characteristics:**
- No physical data loads
- Implemented using SQL views
- Business logic and aggregations applied
- Optimized for BI and analytical queries

**Data Models Supported:**
- Star Schema
- Flat Tables
- Aggregated Tables

---

## 🔄 Data Flow & Lineage
The following diagram illustrates **end-to-end data lineage**, showing how data moves from **source systems → Bronze → Silver → Gold**.

📷 **Data Flow Diagram**  
<img width="1094" height="556" alt="data_flow" src="https://github.com/user-attachments/assets/15a846f2-caec-426f-bbbb-bb3735212ca0" />


**Flow Summary:**
- CRM and ERP data land in Bronze tables
- Silver layer applies cleansing and standardization
- Gold layer integrates Silver tables into dimensions and facts

---

## 🔗 Data Integration Logic
CRM and ERP systems provide overlapping but complementary data.  
The integration logic ensures a **single, consistent business view**.

📷 **Data Integration Diagram**  
<img width="1522" height="746" alt="data_integration" src="https://github.com/user-attachments/assets/72553327-e514-49f2-9316-9d39934442a9" />


**Integration Highlights:**
- CRM provides transactional sales, customer, and product data
- ERP enriches customer attributes (birthdate, country)
- ERP provides product categories and pricing classifications
- Customer and Product entities are unified across systems

---

## ⭐ Sales Data Mart (Star Schema)
The Gold layer implements a **Sales Data Mart** using **dimensional modeling**.

📷 **Star Schema Diagram**  
<img width="1500" height="539" alt="data_model" src="https://github.com/user-attachments/assets/e51b4a6f-7d75-488d-addd-23caf009ce38" />


### Dimension Tables
- `gold.dim_customers`
- `gold.dim_products`

### Fact Table
- `gold.fact_sales`

**Fact Grain:**  
One row per **sales transaction (order_number, product, customer)**

**Measures:**
- `sales_amount`
- `quantity`
- `price`

**Business Rule:**
```sql
sales_amount = quantity * price
```

---

## 🧮 Dimensional Modeling Details

### `dim_customers`
- Surrogate key: `customer_key`
- Attributes:
  - customer_id
  - customer_number
  - first_name
  - last_name
  - gender
  - marital_status
  - birthdate
  - country

### `dim_products`
- Surrogate key: `product_key`
- Attributes:
  - product_id
  - product_number
  - product_name
  - category
  - subcategory
  - product_line
  - maintenance
  - cost
  - start_date

### `fact_sales`
- Foreign keys:
  - product_key
  - customer_key
- Dates:
  - order_date
  - shipping_date
  - due_date
- Metrics:
  - quantity
  - price
  - sales_amount

---

## ⚙️ ETL Implementation
ETL logic is implemented using **SQL scripts and stored procedures**.

**Approach:**
- Layer-specific stored procedures
- Deterministic execution order
- Re-runnable batch processing
- Clear source-to-target mappings

---

## 📂 Repository Structure

```text
├── sql/
│   ├── bronze/
│   │   ├── create_bronze_tables.sql
│   │   ├── load_crm_tables.sql
│   │   ├── load_erp_tables.sql
│   │   └── sp_load_bronze.sql
│   │
│   ├── silver/
│   │   ├── create_silver_tables.sql
│   │   ├── transform_crm_data.sql
│   │   ├── transform_erp_data.sql
│   │   └── sp_load_silver.sql
│   │
│   └── gold/
│       ├── create_dim_customers.sql
│       ├── create_dim_products.sql
│       ├── create_fact_sales.sql
│       └── gold_views.sql
│
├── procedures/
│   ├── sp_bronze_ingestion.sql
│   ├── sp_silver_transformation.sql
│   └── sp_gold_refresh.sql
│
├── docs/
│   ├── data_architecture.png
│   ├── data_flow.png
│   ├── data_integration.png
│   ├── data_model.png
│   └── data_catalog.md
│
├── README.md
```

---

## 📘 Documentation & Governance
- Clear data lineage across layers
- Business-friendly naming conventions
- Schema-level separation
- Column-level definitions supported via data catalog

---

## 🛠️ Technology Stack
- Microsoft SQL Server
- SQL (DDL, DML, Stored Procedures)
- Dimensional Modeling
- Git & GitHub

---

## 🎯 Intended Use
- Data Engineering portfolio project
- Interview discussion reference
- SQL warehouse design showcase
- Analytics and reporting backend template
