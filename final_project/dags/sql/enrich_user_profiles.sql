CREATE OR REPLACE TABLE `{{ params.project_id }}.gold.user_profiles_enriched` AS
    SELECT client_id, first_name, last_name, email, registration_date, state
    FROM `{{ params.project_id }}.silver.customers`
;

ALTER TABLE `{{ params.project_id }}.gold.user_profiles_enriched`
ADD COLUMN birth_date DATE,
ADD COLUMN phone_number STRING;

MERGE INTO `{{ params.project_id }}.gold.user_profiles_enriched` AS target
USING `{{ params.project_id }}.silver.user_profiles` AS source
ON (target.email = source.email)
WHEN MATCHED THEN
    UPDATE SET
        target.client_id = target.client_id,
        target.first_name = SPLIT(source.full_name, ' ')[SAFE_OFFSET(0)],
        target.last_name = SPLIT(source.full_name, ' ')[SAFE_OFFSET(1)],
        target.email = target.email,
        target.registration_date = target.registration_date,
        target.state = source.state,
        target.birth_date = source.birth_date,
        target.phone_number = source.phone_number
