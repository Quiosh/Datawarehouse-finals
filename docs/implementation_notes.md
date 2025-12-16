# Implementation Notes (Pipeline Inventory)

This appendix summarizes the repository scripts and their role in the ELT pipeline.

**Related files:**

- Workflow definition: `workflows/ingestion_flow_main.json`
- Ingestion scripts: `scripts/ingestions/`
- Transform scripts: `scripts/tranforms/`

---

## 1) Runtime / platform assumptions

- Database: PostgreSQL (container service name `db` in scripts).
- Orchestration: Windmill (workflow references scripts by Windmill path such as `f/ingestion/ingest_order_data`).
- Each script typically exposes a `main()` entry point.

---

## 2) Ingestion layer (staging tables)

Below is the ingestion-to-staging mapping as implemented by scripts under `scripts/ingestions/`.

> Note: Several ingestion scripts **DROP and recreate** staging tables each run. Others use **CREATE IF NOT EXISTS + TRUNCATE**.

### Orders (Operations)

- `scripts/ingestions/ingest_order_data.py`
  - Sources: multiple order files across 2020–2023 in varied formats.
  - Loads: `stg_order_data(order_id, user_id, estimated_arrival, transaction_date)`

- `scripts/ingestions/ingest_order_delays.py`
  - Source: `order_delays.html`.
  - Loads: `stg_order_delays(order_id, delay_in_days)`

### Line items (Operations)

- `scripts/ingestions/ingest_line_item_data_products.py`
  - Loads: `stg_line_item_data_products(order_id, product_name, product_id)`

- `scripts/ingestions/ingest_line_item_data_prices.py`
  - Loads: `stg_line_item_data_prices(order_id, price, quantity)`

### Order ↔ merchant/staff linkage (Enterprise)

- `scripts/ingestions/ingest_order_with_merchant_data.py`
  - Loads: `stg_order_with_merchant_data(order_id, merchant_id, staff_id)`

### Marketing

- `scripts/ingestions/ingest_campaign_data.py`
  - Loads: `stg_campaign_data(campaign_id, campaign_name, campaign_description, discount)`

- `scripts/ingestions/ingest_transactional_campaign_data.py`
  - Loads: `stg_transactional_campaign_data(transaction_date, campaign_id, order_id, estimated_arrival, availed)`

### Customer management (users)

- `scripts/ingestions/ingest_user_data.py`
  - Loads: `stg_user_data(user_id, creation_date, name, street, state, city, country, birthdate, gender, device_address, user_type, possible_duplicate, possible_duplicate_of)`

- `scripts/ingestions/ingest_user_job.py`
  - Loads: `stg_user_job(user_id, name, job_title, job_level, possible_duplicate, possible_duplicate_of)`

- `scripts/ingestions/ingest_user_credit_card.py`
  - Loads: `stg_user_credit_card(user_id, name, credit_card_number, issuing_bank)`
  - Implementation detail: creates table if missing, then truncates.

### Master/reference data

- `scripts/ingestions/ingest_product_list.py`
  - Loads: `stg_product_list(product_id, product_name, product_type, price)`
  - Implementation detail: creates table if missing, then truncates.

- `scripts/ingestions/ingest_merchant_data.py`
  - Loads: `stg_merchant_data(merchant_id, creation_date, name, street, state, city, country, contact_number, possible_duplicate, possible_duplicate_of)`

- `scripts/ingestions/ingest_staff_data.py`
  - Loads: `stg_staff_data(staff_id, name, job_level, street, state, city, country, contact_number, creation_date, possible_duplicate, possible_duplicate_of)`

---

## 3) Identity resolution / special transformations

- `scripts/ingestions/transform_resolve_user_collisions.py`
  - Creates (if missing) and maintains a `dim_user` with surrogate keys and effective dating columns (`valid_from`, `valid_to`, `is_current`).
  - Adds `user_key` to several staging tables (idempotent `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`).
  - Propagates user surrogate keys into staging tables:
    - `stg_user_data`, `stg_user_job`, `stg_user_credit_card` by `(user_id, name)` matching
    - `stg_order_data` by date-window logic (`transaction_date` within validity range)

---

## 4) Warehouse transformations (dimensions and facts)

Transform scripts live under `scripts/tranforms/`:

- Dimensions
  - `DIM_DATE.py`
  - `DIM_USER.py`
  - `DIM_PRODUCT.py`
  - `DIM_MERCHANT.py`
  - `DIM_STAFF.py`
  - `DIM_CAMPAIGN.py`

- Facts
  - `FACT_ORDERS.py`
  - `FACT_ORDER_ITEMS` (no file extension in repo; used as a workflow step named `FACT_ORDER_ITEMS`)
  - `FACT_CAMPAIGN_PERFORMANCE.py`

These scripts are expected to:

1. Read from staging tables.
2. Build/load dimension tables (surrogate keys, conformed attributes).
3. Load fact tables after dimensions are available.

The canonical star schema contract is documented in `docs/star_schema.mermaid` and the corresponding appendix `docs/data_dictionary.md`.

---

## 5) Workflow orchestration notes (Windmill)

`workflows/ingestion_flow_main.json` defines a high-level flow:

1. **Parallel ingestion groups** (products+transactional campaign; user credit card + staff + merchant; line items + user data; orders + campaign + user job; order-with-merchant + delays).
2. A cleaning step: `f/clean/testing_cleaning_data_script` (this script path is referenced by the workflow but is **not present** in this repository).
3. **Parallel dimension builds** (DIM_MERCHANT+DIM_DATE; DIM_PRODUCT+DIM_STAFF; DIM_USER+DIM_CAMPAIGN).
4. Fact builds: `FACT_ORDERS`, `FACT_ORDER_ITEMS`, `FACT_CAMPAIGN_PERFORMANCE`.

The workflow references Windmill “script paths” (e.g., `f/ingestion/ingest_order_data`). In this repository, the corresponding code lives under `scripts/ingestions/` and `scripts/tranforms/`.
