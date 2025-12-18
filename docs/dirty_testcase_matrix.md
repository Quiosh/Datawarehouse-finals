# Dirty ingestion test fixtures: expected outcomes

This document defines what each row in the `dirty_*` CSV fixtures is intended to test, based on the cleaning/validation logic in the `scripts/testcased/ingest_dirty_*.py` scripts.

## 1) Orders

Fixture: `datasets/Test Files/dirty_order_data.csv`

Ingestion script logic:
- Drops rows where `Estimated_arrival` becomes NULL after stripping non-digits and coercing to numeric.
- Drops rows where `Transaction_date` cannot be parsed to datetime.
- Drops rows where `Order_id` is missing/blank.
- Inserts remaining rows row-by-row into `stg_order_data(order_id TEXT, user_id TEXT, estimated_arrival INTEGER, transaction_date TIMESTAMP)`.

Row expectations:
1. `O-1001,U-501,5,2023-07-01 12:00:00`
   - Expected: **VALID (kept + insert success)**
2. `O-1002,U-502,15days,2023-07-02`
   - Expected: **VALID (kept + insert success)**
   - Rationale: `15days` becomes `15` after non-digit stripping.
3. `,U-503,7,2023-07-03`
   - Expected: **INVALID (dropped by cleaning)**
   - Rationale: missing `Order_id`.
4. `O-1004,U-504,N/A,2023-07-04`
   - Expected: **INVALID (dropped by cleaning)**
   - Rationale: `N/A` -> empty -> NULL after numeric coercion.
5. `O-1005,U-505,9,not-a-date`
   - Expected: **INVALID (dropped by cleaning)**
   - Rationale: invalid `Transaction_date`.
6. `O-OVR,U-506,999999999999,2023-07-05`
   - Expected: **INVALID (INSERT-time fail)**
   - Rationale: passes cleaning (numeric), but should overflow Postgres `INTEGER` during insert.

## 2) Product list

Fixture: `datasets/Test Files/dirty_product_list.csv`

Ingestion script logic:
- Drops rows where `Price` cannot be coerced to numeric.
- Drops rows where `Product_id` is missing/blank.
- Inserts remaining rows row-by-row into `stg_product_list(product_id TEXT, product_name TEXT, product_type TEXT, price NUMERIC)`.

Row expectations:
1. `P-100,Widget,kitchen,12.50`
   - Expected: **VALID (kept + insert success)**
2. `P-101, Wok ,readymade_breakfast,9.99`
   - Expected: **VALID (kept + insert success)**
3. `,MissingIdItem,tools,1.00`
   - Expected: **INVALID (dropped by cleaning)**
   - Rationale: missing `Product_id`.
4. `P-103,BadPriceItem,tools,free`
   - Expected: **INVALID (dropped by cleaning)**
   - Rationale: `free` -> NULL after numeric coercion.
5. `P-INF,InfinityPriceItem,tools,inf`
   - Expected: **INVALID (INSERT-time fail)**
   - Rationale: pandas typically parses `inf` as a float infinity (not NULL), but Postgres `NUMERIC` generally rejects infinity.

## 3) Line item (order ↔ product mapping)

Fixture: `datasets/Test Files/dirty_line_item_data_products.csv`

Ingestion script logic:
- Drops rows where `Order_id` or `Product_id` is missing/blank.
- Inserts remaining rows row-by-row into `stg_line_item_data_products(order_id TEXT, product_name TEXT, product_id TEXT)`.

Row expectations:
1. `O-1001,Widget,P-100`
   - Expected: **VALID (kept + insert success)**
2. `O-1001,Widget,P-100`
   - Expected: **VALID (kept + insert success)**
   - Note: duplicate row is intentional (the script does not deduplicate).
3. `,NoOrderId,P-101`
   - Expected: **INVALID (dropped by cleaning)**
4. `O-1002,NoProductId,`
   - Expected: **INVALID (dropped by cleaning)**

## Notes on INSERT-time failure cases

INSERT-time failures depend on the target Postgres types and driver conversions:
- Orders: overflow on `estimated_arrival INTEGER` should reliably fail when using a value > 2,147,483,647.
- Product list: `price NUMERIC` rejecting infinity depends on how pandas/psycopg2 binds the `inf` value.
