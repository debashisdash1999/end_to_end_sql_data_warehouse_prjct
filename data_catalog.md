# 📘 Data Catalog – Gold Layer

## Overview
The Gold Layer represents the **business-level analytical data model**, designed to support reporting and analytics use cases.  
It consists of **dimension tables** and **fact tables** built using dimensional modeling principles.

This catalog documents **table purpose, column definitions, data types, and business meaning** for the Gold layer.

---

## 1. `gold.dim_customers`

### Purpose
Stores customer details enriched with demographic and geographic attributes, integrated from CRM and ERP systems.

### Columns

| Column Name      | Data Type      | Description |
|------------------|----------------|-------------|
| customer_key     | INT            | Surrogate key uniquely identifying each customer record in the dimension table. |
| customer_id      | INT            | Unique numerical identifier assigned to each customer. |
| customer_number  | NVARCHAR(50)   | Alphanumeric identifier used for customer tracking and reference. |
| first_name       | NVARCHAR(50)   | Customer’s first name. |
| last_name        | NVARCHAR(50)   | Customer’s last name. |
| country          | NVARCHAR(50)   | Country of residence (e.g., Australia). |
| marital_status   | NVARCHAR(50)   | Marital status (e.g., Married, Single). |
| gender           | NVARCHAR(50)   | Gender of the customer (e.g., Male, Female, n/a). |
| birthdate        | DATE           | Customer’s date of birth (YYYY-MM-DD). |
| create_date      | DATE           | Date when the customer record was created. |

---

## 2. `gold.dim_products`

### Purpose
Provides descriptive information about products, including category, pricing attributes, and lifecycle details.

### Columns

| Column Name           | Data Type      | Description |
|-----------------------|----------------|-------------|
| product_key           | INT            | Surrogate key uniquely identifying each product record. |
| product_id            | INT            | Unique identifier assigned to the product. |
| product_number        | NVARCHAR(50)   | Alphanumeric product code used for tracking and inventory. |
| product_name          | NVARCHAR(50)   | Descriptive product name including type and specifications. |
| category_id           | NVARCHAR(50)   | Identifier representing the product category. |
| category              | NVARCHAR(50)   | High-level product category (e.g., Bikes, Components). |
| subcategory           | NVARCHAR(50)   | Detailed product classification within a category. |
| maintenance_required  | NVARCHAR(50)   | Indicates whether maintenance is required (Yes/No). |
| cost                  | INT            | Base cost of the product. |
| product_line          | NVARCHAR(50)   | Product line or series (e.g., Road, Mountain). |
| start_date            | DATE           | Date when the product became available. |

---

## 3. `gold.fact_sales`

### Purpose
Stores transactional sales data for analytical and reporting purposes.

### Fact Grain
One record per **sales transaction line item**, defined by:
- order_number
- product
- customer

### Columns

| Column Name     | Data Type     | Description |
|-----------------|---------------|-------------|
| order_number    | NVARCHAR(50)  | Unique sales order identifier (e.g., SO54496). |
| product_key     | INT           | Foreign key referencing `dim_products`. |
| customer_key    | INT           | Foreign key referencing `dim_customers`. |
| order_date      | DATE          | Date when the order was placed. |
| shipping_date   | DATE          | Date when the order was shipped. |
| due_date        | DATE          | Payment due date. |
| quantity        | INT           | Number of units sold. |
| price           | INT           | Price per unit. |
| sales_amount    | INT           | Total sales value (quantity × price). |

---

## Catalog Scope
- Covers **Gold layer only**
- Focused on analytics and reporting use cases
- Designed to support BI tools and ad-hoc SQL analysis


