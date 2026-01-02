# 📘 Data Catalog – Gold Layer

## Overview
The Gold Layer represents the **business-level analytical data model**, designed to support reporting, dashboards, and ad-hoc analysis.  
It is built using **dimensional modeling principles**, consisting of **dimension views** for descriptive context and **fact views** for measurable business events.

This catalog documents:
- Table purpose and business role  
- Grain definition  
- Source systems  
- Column-level definitions and business meaning  

---

## 1. `gold.dim_customers`

### Table Type
**Dimension**

### Why Dimension
- Contains descriptive attributes about customers
- Used to slice, filter, and group sales facts
- Does not store transactional or additive measures
- Low to medium cardinality

### Grain
One record per **unique customer**

### Business Role
Provides customer context for sales analytics, customer demographics, and geographic reporting.

### Source Tables
- `silver.crm_cust_info` – System of record for customer identity  
- `silver.erp_cust_az12` – Demographic attributes  
- `silver.erp_loc_a101` – Geographic attributes  

### Columns

| Column Name      | Data Type     | Description |
|------------------|---------------|-------------|
| customer_key     | INT           | Surrogate key used for joins in the star schema. |
| customer_id      | INT           | Business identifier of the customer from CRM. |
| customer_number  | NVARCHAR(50)  | Alphanumeric customer reference number. |
| first_name       | NVARCHAR(50)  | Customer’s first name. |
| last_name        | NVARCHAR(50)  | Customer’s last name. |
| country          | NVARCHAR(50)  | Country of residence derived from ERP location data. |
| marital_status   | NVARCHAR(50)  | Standardized marital status (e.g., Single, Married). |
| gender           | NVARCHAR(50)  | Customer gender, primarily from CRM with ERP as fallback. |
| birthdate        | DATE          | Customer’s date of birth. |
| create_date      | DATE          | Date when the customer record was created in CRM. |

---

## 2. `gold.dim_products`

### Table Type
**Dimension**

### Why Dimension
- Contains descriptive product attributes
- Enables analysis by product, category, and product line
- Does not contain transactional measures

### Grain
One record per **active product**

### Business Role
Provides product context for sales analysis, category performance, and product lifecycle reporting.

### Source Tables
- `silver.crm_prd_info` – Product master data  
- `silver.erp_px_cat_g1v2` – Product category and maintenance attributes  

### Columns

| Column Name      | Data Type     | Description |
|------------------|---------------|-------------|
| product_key      | INT           | Surrogate key uniquely identifying each product. |
| product_id       | INT           | Business identifier of the product from CRM. |
| product_number   | NVARCHAR(50)  | Alphanumeric product code used in transactions. |
| product_name     | NVARCHAR(50)  | Descriptive product name. |
| category_id      | NVARCHAR(50)  | Identifier linking the product to its category. |
| category         | NVARCHAR(50)  | High-level product category (e.g., Bikes, Components). |
| subcategory      | NVARCHAR(50)  | Detailed product classification within a category. |
| maintenance      | NVARCHAR(50)  | Indicates whether the product requires maintenance. |
| cost             | INT           | Base cost of the product. |
| product_line     | NVARCHAR(50)  | Product line classification (e.g., Road, Mountain). |
| start_date       | DATE          | Date when the product became active. |

---

## 3. `gold.fact_sales`

### Table Type
**Fact**

### Why Fact
- Stores measurable business events (sales transactions)
- Contains numeric, additive metrics
- References dimension tables using surrogate keys
- Represents the lowest level of business grain

### Grain
One record per **sales transaction line item**, defined by:
- order_number  
- product  
- customer  

### Business Role
Central table for revenue, sales performance, and operational analytics.

### Source Tables
- `silver.crm_sales_details` – Transactional sales data  
- `gold.dim_customers` – Customer dimension  
- `gold.dim_products` – Product dimension  

### Columns

| Column Name     | Data Type     | Description |
|-----------------|---------------|-------------|
| order_number    | NVARCHAR(50)  | Unique sales order identifier. |
| product_key     | INT           | Foreign key referencing `gold.dim_products`. |
| customer_key    | INT           | Foreign key referencing `gold.dim_customers`. |
| order_date      | DATE          | Date when the order was placed. |
| shipping_date   | DATE          | Date when the order was shipped. |
| due_date        | DATE          | Payment due date. |
| quantity        | INT           | Number of units sold. |
| price           | INT           | Price per unit. |
| sales_amount    | INT           | Total sales value (quantity × price). |

---

## Catalog Scope
- Covers **Gold layer only**
- Designed for analytics and reporting workloads
- Optimized for BI tools and ad-hoc SQL queries
- Reflects business-facing data structures
