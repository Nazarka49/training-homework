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

SELECT *
FROM (
    SELECT
        CAST(CustomerId AS INTEGER) AS client_id,
        CASE
            WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}/\d{2}/\d{2}$') THEN PARSE_DATE('%Y/%m/%d', PurchaseDate)
            WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-[A-Za-z]{3}-\d{2}$') THEN PARSE_DATE('%Y-%b-%d', PurchaseDate)
            ELSE SAFE.PARSE_DATE('%Y-%m-%d', PurchaseDate)
        END AS purchase_date,
        Product AS product_name,
        CAST(REGEXP_REPLACE(Price, r'[^0-9]', '') AS INTEGER) AS price
    FROM `{{ params.project_id }}.bronze.sales`
)
WHERE DATE(purchase_date) = "{{ ds }}"
;