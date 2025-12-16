# Data Dictionary (Star Schema)

**Applies to:** Dimensional model described in [`docs/star_schema.mermaid`](star_schema.mermaid:1).

**Notes**

- Types are expressed in a PostgreSQL-friendly style (e.g., `text`, `int`, `decimal`).
- Some transformation implementations may include additional attributes not shown in the canonical ERD; the ERD is treated as the authoritative contract for this dictionary.

---

## Conformed dimensions

### `DIM_DATE`

**Purpose:** Calendar dimension for consistent time-based slicing.

**Grain:** One row per calendar date.

**Primary key:** `date_key` (integer, typically `YYYYMMDD`).

| Column | Type | Key | Description |
|---|---:|---|---|
| `date_key` | `int` | PK | Surrogate date key (recommended format: `YYYYMMDD`). |
| `full_date` | `date` |  | Calendar date. |
| `year` | `int` |  | Calendar year. |
| `quarter` | `int` |  | Quarter of year (1–4). |
| `month` | `int` |  | Month of year (1–12). |
| `day` | `int` |  | Day of month (1–31). |
| `is_weekend` | `boolean` |  | True if Saturday/Sunday (based on locale/business rule). |

---

### `DIM_USER`

**Purpose:** Customer/user attributes for segmentation and rollups.

**Grain:** One row per user (surrogate key). Depending on implementation, this may be Type-1 (latest snapshot) or Type-2 (history); facts reference `user_key`.

**Primary key:** `user_key`.

| Column | Type | Key | Description |
|---|---:|---|---|
| `user_key` | `int` | PK | Surrogate user key used in facts. |
| `source_user_id` | `text` |  | Business key from source systems (staging `user_id`). |
| `name` | `text` |  | User/customer name. |
| `birthdate` | `date` |  | Date of birth (if available/valid). |
| `gender` | `text` |  | Gender (as provided by source). |
| `user_type` | `text` |  | User category/type. |
| `city` | `text` |  | City. |
| `state` | `text` |  | State/region. |
| `country` | `text` |  | Country. |
| `job_title` | `text` |  | Job title (if available). |
| `job_level` | `text` |  | Job seniority/level (if available). |

---

### `DIM_PRODUCT`

**Purpose:** Product catalog dimension.

**Grain:** One row per product.

**Primary key:** `product_key`.

| Column | Type | Key | Description |
|---|---:|---|---|
| `product_key` | `int` | PK | Surrogate product key. |
| `product_id` | `text` |  | Source product identifier (business key). |
| `product_name` | `text` |  | Product name. |
| `product_type` | `text` |  | Product category/type. |
| `base_price` | `decimal` |  | Base/list price (currency as defined by business). |

---

### `DIM_MERCHANT`

**Purpose:** Merchant/seller attributes.

**Grain:** One row per merchant.

**Primary key:** `merchant_key`.

| Column | Type | Key | Description |
|---|---:|---|---|
| `merchant_key` | `int` | PK | Surrogate merchant key. |
| `source_merchant_id` | `text` |  | Source merchant identifier (business key). |
| `name` | `text` |  | Merchant name. |
| `city` | `text` |  | City. |
| `state` | `text` |  | State/region. |
| `country` | `text` |  | Country. |

---

### `DIM_STAFF`

**Purpose:** Internal staff attributes for operational analytics.

**Grain:** One row per staff member.

**Primary key:** `staff_key`.

| Column | Type | Key | Description |
|---|---:|---|---|
| `staff_key` | `int` | PK | Surrogate staff key. |
| `source_staff_id` | `text` |  | Source staff identifier (business key). |
| `name` | `text` |  | Staff name. |
| `job_level` | `text` |  | Staff job level/seniority. |
| `city` | `text` |  | City. |
| `country` | `text` |  | Country. |

---

### `DIM_CAMPAIGN`

**Purpose:** Marketing campaign attributes.

**Grain:** One row per campaign.

**Primary key:** `campaign_key`.

| Column | Type | Key | Description |
|---|---:|---|---|
| `campaign_key` | `int` | PK | Surrogate campaign key. |
| `campaign_id` | `text` |  | Source campaign identifier (business key). |
| `campaign_name` | `text` |  | Campaign name. |
| `description` | `text` |  | Campaign description. |
| `discount` | `decimal` |  | Discount value/percentage as numeric. |

---

## Fact tables

### `FACT_ORDERS`

**Purpose:** Order-level facts for transaction analysis.

**Grain:** One row per order (`order_id`) per associated dimensional keys.

**Primary key:** `order_key`.

**Foreign keys:** `user_key`, `merchant_key`, `staff_key`, `campaign_key`, `date_key`.

| Column | Type | Key | Description |
|---|---:|---|---|
| `order_key` | `int` | PK | Surrogate key for fact row.
| `order_id` | `text` |  | Business identifier for the order.
| `user_key` | `int` | FK | References `DIM_USER.user_key`.
| `merchant_key` | `int` | FK | References `DIM_MERCHANT.merchant_key`.
| `staff_key` | `int` | FK | References `DIM_STAFF.staff_key`.
| `campaign_key` | `int` | FK | References `DIM_CAMPAIGN.campaign_key`.
| `date_key` | `int` | FK | References `DIM_DATE.date_key` (order transaction date).
| `delay_in_days` | `int` |  | Delivery delay in days (if available).
| `total_amount` | `decimal` |  | Total order amount.

---

### `FACT_ORDER_ITEMS`

**Purpose:** Line-item facts supporting product-level analytics.

**Grain:** One row per order-item (line) per product, per date (and associated dimensions).

**Primary key:** `order_item_key`.

**Foreign keys:** `product_key`, `user_key`, `merchant_key`, `campaign_key`, `date_key`.

| Column | Type | Key | Description |
|---|---:|---|---|
| `order_item_key` | `int` | PK | Surrogate key for line item.
| `order_id` | `text` |  | Business identifier linking to the order.
| `product_key` | `int` | FK | References `DIM_PRODUCT.product_key`.
| `user_key` | `int` | FK | References `DIM_USER.user_key`.
| `merchant_key` | `int` | FK | References `DIM_MERCHANT.merchant_key`.
| `campaign_key` | `int` | FK | References `DIM_CAMPAIGN.campaign_key`.
| `date_key` | `int` | FK | References `DIM_DATE.date_key`.
| `quantity` | `int` |  | Units purchased.
| `unit_price` | `decimal` |  | Unit price at purchase time.
| `total_price` | `decimal` |  | Extended price (`quantity * unit_price`).

---

### `FACT_CAMPAIGN_PERFORMANCE`

**Purpose:** Aggregated campaign effectiveness facts.

**Grain:** One row per campaign per date.

**Primary key:** `campaign_perf_key`.

**Foreign keys:** `campaign_key`, `date_key`.

| Column | Type | Key | Description |
|---|---:|---|---|
| `campaign_perf_key` | `int` | PK | Surrogate key for the aggregate row.
| `campaign_key` | `int` | FK | References `DIM_CAMPAIGN.campaign_key`.
| `date_key` | `int` | FK | References `DIM_DATE.date_key`.
| `total_orders` | `int` |  | Total orders attributed to the campaign on that date.
| `total_revenue` | `decimal` |  | Revenue attributed to the campaign on that date.
| `average_order_value` | `decimal` |  | `total_revenue / total_orders` (if defined).
| `unique_customers` | `int` |  | Count of distinct customers involved.
