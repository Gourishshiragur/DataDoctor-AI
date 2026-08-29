# Demo Datasets

Each dataset is intentionally dirty so the Silver-layer self-healing engine has real
work to do. All are synthetic — no real customer/patient/account data.

## Retail — Store Transactions (`datasets/retail/transactions.csv`)
Point-of-sale transactions. Inconsistent store codes (`ST-002` vs `st-003` vs `ST_005`),
~6% null `unit_price`, ~3% negative `quantity`, and duplicated `order_id`s injected
periodically to simulate upstream reprocessing bugs.

## Banking — Customer Accounts (`datasets/banking/accounts.csv`)
Account records with a small fraction of malformed IBANs, ~5% null `balance`, negative
`age` anomalies (data entry errors), and mixed-case `account_type` values
(`Savings`/`savings`/`CHECKING`).

## Healthcare — Patient Visits (`datasets/healthcare/visits.csv`)
Visit logs with three different date formats mixed in the same `visit_date` column
(simulating multiple source systems), ~7% null `heart_rate` (sensor dropout), a small
fraction of `temperature_c == 0.0` (sensor fault), and `systolic_bp == 999` outliers
(instrument error code leaking into the data).

## E-Commerce — Order Events (`datasets/ecommerce/orders.csv`)
Order + shipping events with a deliberately smaller pool of unique emails than orders
(repeat customers, but also a stress-test for dedup logic), mixed-case `currency`
values (`USD`/`usd`/`Usd`), ~8% null `shipping_date`, and ~3% null `amount`.

## Manufacturing — Sensor & QA Logs (`datasets/manufacturing/sensor_qa.csv`)
Machine sensor readings with mixed-case `shift` values (`Morning`/`morning`/`NIGHT`),
~5% null `temperature_c` (sensor dropout), and ~1.5% `vibration_mm_s` spikes at 99.9
(sensor malfunction, not a real reading) that the outlier-capping rule should catch.
