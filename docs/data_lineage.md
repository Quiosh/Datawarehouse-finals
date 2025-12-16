# Data Lineage (Source → Staging → Dimensional Model)

This appendix provides a high-level lineage map from departmental source files to staging tables and downstream dimensions/facts.

**Important:** This lineage is based on the implemented ingestion scripts under `scripts/ingestions/` and the star schema contract in `docs/star_schema.mermaid`.

---

## 1) Lineage map (overview)

```mermaid
flowchart LR

  %% =====================
  %% Sources
  %% =====================
  subgraph SRC["Departmental sources (datasets/)"]
    SRC_OPS["Operations Dept<br/>order_data_*<br/>line_item_data_*<br/>order_delays.html"]
    SRC_ENT["Enterprise Dept<br/>merchant_data.html<br/>staff_data.html<br/>order_with_merchant_data*"]
    SRC_CMD["Customer Mgmt Dept<br/>user_data.json<br/>user_job.csv<br/>user_credit_card.pickle"]
    SRC_MKT["Marketing Dept<br/>campaign_data.csv<br/>transactional_campaign_data.csv"]
    SRC_BD["Business Dept<br/>product_list.xlsx"]
  end

  %% =====================
  %% Ingestion to staging
  %% =====================
  subgraph STG["Staging tables (PostgreSQL)"]
    STG_ORDER[stg_order_data]
    STG_DELAY[stg_order_delays]

    STG_LIP[stg_line_item_data_products]
    STG_LIPR[stg_line_item_data_prices]

    STG_OWM[stg_order_with_merchant_data]

    STG_USER[stg_user_data]
    STG_USERJOB[stg_user_job]
    STG_UCC[stg_user_credit_card]

    STG_CAMP[stg_campaign_data]
    STG_TCD[stg_transactional_campaign_data]

    STG_PROD[stg_product_list]

    STG_MER[stg_merchant_data]
    STG_STAFF[stg_staff_data]
  end

  %% Source -> staging edges
  SRC_OPS -->|ingest_order_data| STG_ORDER
  SRC_OPS -->|ingest_order_delays| STG_DELAY
  SRC_OPS -->|ingest_line_item_data_products| STG_LIP
  SRC_OPS -->|ingest_line_item_data_prices| STG_LIPR

  SRC_ENT -->|ingest_order_with_merchant_data| STG_OWM
  SRC_ENT -->|ingest_merchant_data| STG_MER
  SRC_ENT -->|ingest_staff_data| STG_STAFF

  SRC_CMD -->|ingest_user_data| STG_USER
  SRC_CMD -->|ingest_user_job| STG_USERJOB
  SRC_CMD -->|ingest_user_credit_card| STG_UCC

  SRC_MKT -->|ingest_campaign_data| STG_CAMP
  SRC_MKT -->|ingest_transactional_campaign_data| STG_TCD

  SRC_BD -->|ingest_product_list| STG_PROD

  %% =====================
  %% Identity resolution
  %% =====================
  subgraph IDR[Identity resolution]
    RESOLVE["transform_resolve_user_collisions<br/>(user surrogate key + validity windows)"]
  end

  STG_USER --> RESOLVE
  STG_USERJOB --> RESOLVE
  STG_UCC --> RESOLVE
  STG_ORDER --> RESOLVE

  %% =====================
  %% Dimensions
  %% =====================
  subgraph DIM[Dimensions]
    DIM_DATE[DIM_DATE]
    DIM_USER[DIM_USER]
    DIM_PROD[DIM_PRODUCT]
    DIM_MER[DIM_MERCHANT]
    DIM_STAFF[DIM_STAFF]
    DIM_CAMP[DIM_CAMPAIGN]
  end

  RESOLVE -->|user_key| DIM_USER
  STG_PROD --> DIM_PROD
  STG_MER --> DIM_MER
  STG_STAFF --> DIM_STAFF
  STG_CAMP --> DIM_CAMP

  STG_ORDER -->|transaction_date| DIM_DATE
  STG_TCD -->|transaction_date| DIM_DATE

  %% =====================
  %% Facts
  %% =====================
  subgraph FACT[Facts]
    F_ORD[FACT_ORDERS]
    F_ITEM[FACT_ORDER_ITEMS]
    F_CPERF[FACT_CAMPAIGN_PERFORMANCE]
  end

  %% Fact dependencies
  STG_ORDER --> F_ORD
  STG_DELAY --> F_ORD
  STG_OWM --> F_ORD
  STG_TCD --> F_ORD

  STG_LIP --> F_ITEM
  STG_LIPR --> F_ITEM
  STG_ORDER --> F_ITEM
  STG_OWM --> F_ITEM
  STG_TCD --> F_ITEM

  STG_TCD --> F_CPERF

  %% FK lookups
  DIM_DATE --> F_ORD
  DIM_USER --> F_ORD
  DIM_MER --> F_ORD
  DIM_STAFF --> F_ORD
  DIM_CAMP --> F_ORD

  DIM_DATE --> F_ITEM
  DIM_USER --> F_ITEM
  DIM_PROD --> F_ITEM
  DIM_MER --> F_ITEM
  DIM_CAMP --> F_ITEM

  DIM_DATE --> F_CPERF
  DIM_CAMP --> F_CPERF
```

---

## 2) Narrative lineage (by subject area)

### Orders analytics

- **Primary source:** Operations `order_data_*` files.
- **Staging:** `stg_order_data`.
- **Enrichment:**
  - Delay metrics from `stg_order_delays`.
  - Merchant/staff relationships from `stg_order_with_merchant_data`.
  - Campaign relationship from `stg_transactional_campaign_data`.
  - User surrogate key propagation from the identity resolution step.
- **Warehouse output:** `FACT_ORDERS`.

### Product / line-item analytics

- **Primary sources:** Operations `line_item_data_products*` and `line_item_data_prices*`.
- **Staging:** `stg_line_item_data_products`, `stg_line_item_data_prices`.
- **Reference data:** product master list loaded to `stg_product_list`.
- **Warehouse output:** `FACT_ORDER_ITEMS` joined to `DIM_PRODUCT`.

### Campaign analytics

- **Sources:**
  - Campaign attributes from `campaign_data.csv` → `stg_campaign_data` → `DIM_CAMPAIGN`.
  - Transactional campaign attribution from `transactional_campaign_data.csv` → `stg_transactional_campaign_data`.
- **Warehouse output:** `FACT_CAMPAIGN_PERFORMANCE` (aggregations by campaign and date).

### Customer/user analytics

- **Sources:** `user_data.json`, `user_job.csv`, `user_credit_card.pickle`.
- **Staging:** `stg_user_data`, `stg_user_job`, `stg_user_credit_card`.
- **Identity resolution:** a surrogate key + validity window approach establishes consistent user keys.
- **Warehouse output:** `DIM_USER` and consistent `user_key` references in facts.
