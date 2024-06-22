CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.silver.customers`
    (client_id INTEGER, first_name STRING, last_name STRING, email STRING, registration_date DATE, state STRING)
;

DELETE FROM `{{ params.project_id }}.silver.customers`
WHERE TRUE
;

INSERT `{{ params.project_id }}.silver.customers` (
    client_id,
    first_name,
    last_name,
    email,
    registration_date,
    state
)
SELECT
    CAST(Id AS INTEGER) AS client_id,
    FirstName AS first_name,
    LastName AS last_name,
    Email AS email,
    CAST(RegistrationDate AS DATE) AS registration_date,
    State AS state
FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY Id) as rk FROM `{{ params.project_id }}.bronze.customers`)
WHERE rk=1
;