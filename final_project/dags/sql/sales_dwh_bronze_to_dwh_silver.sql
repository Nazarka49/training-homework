CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.silver.sales`
    (client_id INTEGER, purchase_date DATE, product STRING, price INTEGER)
PARTITION BY purchase_date;

DELETE FROM `{{ params.project_id }}.silver.sales`
WHERE DATE(purchase_date) = "{{ ds }}"
;

INSERT `{{ params.project_id }}.silver.sales` (
    client_id,
    purchase_date,
    product,
    price
)
SELECT
    CAST(CustomerId AS INTEGER) AS client_id,
    CAST(PurchaseDate AS DATE) AS purchase_date,
    Product AS product_name,
    CAST(REGEXP_REPLACE(Price, r'[^0-9]', '') AS INTEGER) AS price
FROM `{{ params.project_id }}.bronze.sales`
;