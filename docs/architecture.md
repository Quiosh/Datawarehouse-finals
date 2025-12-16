# Data Warehouse Architecture

## 1. High-Level Architecture

This Data Warehouse solution follows a modern ELT (Extract, Load, Transform) pipeline architecture designed to consolidate data from disparate departmental sources into a unified analytical store.

```mermaid
graph LR
    subgraph "Data Sources"
        BD["Business Dept<br>(Excel)"]
        CMD["Customer Mgmt<br>(Pickle, JSON, CSV)"]
        ENT["Enterprise Dept<br>(HTML, Parquet, CSV)"]
        MKT["Marketing Dept<br>(CSV)"]
        OPS["Operations Dept<br>(Parquet, Pickle, CSV, Excel, JSON, HTML)"]
    end
    subgraph "Ingestion Layer"
        Scripts[Python Ingestion Scripts]
    end
    subgraph "Data Warehouse (PostgreSQL)"
        Raw[Raw / Staging Tables]
        Transform["Transformation Logic<br>(dbt / SQL / Python)"]
        DWH["Dimensional Model<br>(Star Schema)"]
    end
    subgraph "Orchestration"
        Windmill[Windmill / Workflow Engine]
    end
    subgraph "BI & Analysis"
        Metabase[Metabase Dashboard]
    end
    BD --> Scripts
    CMD --> Scripts
    ENT --> Scripts
    MKT --> Scripts
    OPS --> Scripts
    Scripts --> Raw
    Raw --> Transform
    Transform --> DWH
    DWH --> Metabase
    Windmill --> Scripts
    Windmill --> Transform
```

### Components
*   **Data Sources**: Diverse file formats (CSV, Excel, JSON, Parquet, Pickle, HTML) across multiple departments.
*   **Ingestion Layer**: Custom Python scripts responsible for reading raw files and loading them into the staging area of the warehouse.
*   **Storage & Warehousing**: PostgreSQL serves as the central data warehouse, hosting both raw staging data and the final dimensional models.
*   **Orchestration**: Windmill is used to schedule and manage the dependency graph of ingestion and transformation tasks.
*   **BI & Visualization**: Metabase connects to the Data Warehouse to provide insights, dashboards, and reporting capabilities.

---

## 2. Methodology

**Chosen Methodology: Kimball (Dimensional Modeling)**

We have adopted the **Kimball** methodology for this Data Warehouse. This approach focuses on delivering data that is understandable and fast to query for end-users.

*   **Bottom-Up Approach**: We build the warehouse by identifying key business processes and modeling them as **Fact** tables, surrounded by descriptive **Dimension** tables.
*   **Star Schema**: The physical implementation uses a Star Schema design to optimize for read performance and simplicity in reporting.
*   **Conformed Dimensions**: Shared dimensions (like `DIM_DATE`, `DIM_USER`, `DIM_PRODUCT`) are designed to be used across multiple facts, ensuring consistency in reporting across different business areas.

---

## 3. Data Models

### 3.1 Conceptual Data Model
At a high level, the business tracks **Orders** and **Campaign Performance**.

*   **Orders**: The central transaction event. It involves a *User* buying a *Product* from a *Merchant*, processed by a *Staff* member, potentially influenced by a *Campaign*.
*   **Campaign Performance**: Aggregated metrics tracking how well marketing campaigns are performing in terms of revenue and customer engagement.

### 3.2 Logical Data Model
*   **Facts**:
    *   `Fact_Orders`: Transactional facts containing metrics like `total_amount`, `delay_in_days`.
    *   `Fact_Order_Items`: Line-item details containing `quantity`, `unit_price`, `total_price`.
    *   `Fact_Campaign_Performance`: Aggregate facts containing `total_revenue`, `total_orders`.
*   **Dimensions**:
    *   `Dim_User`: Customer demographics (Name, City, Job).
    *   `Dim_Product`: Product catalog details (Name, Type, Base Price).
    *   `Dim_Merchant`: Seller information.
    *   `Dim_Staff`: Internal staff details.
    *   `Dim_Campaign`: Marketing campaign attributes (Discount, Description).
    *   `Dim_Date`: Calendar attributes for temporal analysis.

### 3.3 Physical Data Model (Star Schema)

The following Entity Relationship Diagram (ERD) represents the physical Star Schema implementation in the PostgreSQL Data Warehouse.

```mermaid
erDiagram
    %% Fact Tables
    FACT_CAMPAIGN_PERFORMANCE {
        int campaign_perf_key PK
        int campaign_key FK
        int date_key FK
        int total_orders
        decimal total_revenue
        decimal average_order_value
        int unique_customers
    }

    FACT_ORDERS {
        int order_key PK
        text order_id
        int user_key FK
        int merchant_key FK
        int staff_key FK
        int campaign_key FK
        int date_key FK
        int delay_in_days
        decimal total_amount
    }

    FACT_ORDER_ITEMS {
        int order_item_key PK
        text order_id
        int product_key FK
        int user_key FK
        int merchant_key FK
        int campaign_key FK
        int date_key FK
        int quantity
        decimal unit_price
        decimal total_price
    }

    %% Dimensions
    DIM_CAMPAIGN {
        int campaign_key PK
        text campaign_id
        text campaign_name
        text description
        decimal discount
    }

    DIM_PRODUCT {
        int product_key PK
        text product_id
        text product_name
        text product_type
        decimal base_price
    }

    DIM_USER {
        int user_key PK
        text source_user_id
        text name
        date birthdate
        text gender
        text user_type
        text city
        text state
        text country
        text job_title
        text job_level
    }

    DIM_MERCHANT {
        int merchant_key PK
        text source_merchant_id
        text name
        text city
        text state
        text country
    }

    DIM_STAFF {
        int staff_key PK
        text source_staff_id
        text name
        text job_level
        text city
        text country
    }

    DIM_DATE {
        int date_key PK
        date full_date
        int year
        int quarter
        int month
        int day
        boolean is_weekend
    }

    %% Relationships
    FACT_CAMPAIGN_PERFORMANCE }|..|{ DIM_CAMPAIGN : "evaluates"
    FACT_CAMPAIGN_PERFORMANCE }|..|{ DIM_DATE : "measured on"

    FACT_ORDERS }|..|{ DIM_USER : "placed by"
    FACT_ORDERS }|..|{ DIM_MERCHANT : "sold by"
    FACT_ORDERS }|..|{ DIM_STAFF : "processed by"
    FACT_ORDERS }|..|{ DIM_CAMPAIGN : "attributed to"
    FACT_ORDERS }|..|{ DIM_DATE : "occurred on"

    FACT_ORDER_ITEMS }|..|{ DIM_PRODUCT : "contains"
    FACT_ORDER_ITEMS }|..|{ DIM_USER : "placed by"
    FACT_ORDER_ITEMS }|..|{ DIM_MERCHANT : "sold by"
    FACT_ORDER_ITEMS }|..|{ DIM_CAMPAIGN : "attributed to"
    FACT_ORDER_ITEMS }|..|{ DIM_DATE : "occurred on"
```