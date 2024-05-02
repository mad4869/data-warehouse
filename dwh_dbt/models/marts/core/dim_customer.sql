{{ config(schema='final') }}

WITH stg_customer AS (
    SELECT
        customer_id AS nk_customer_id,
        first_name,
        last_name,
        CONCAT(first_name, ' ', last_name) AS full_name,
        email
    FROM {{ ref("stg_dwh_customer") }}
),

stg_customer_address AS (
    SELECT *
    FROM {{ ref("stg_dwh_customer_address") }}
),

stg_address AS (
    SELECT *
    FROM {{ ref("stg_dwh_address") }}
),

stg_address_status AS (
    SELECT *
    FROM {{ ref("stg_dwh_address_status") }}
),

stg_country AS (
    SELECT *
    FROM {{ ref("stg_dwh_country") }}
),

complete_customer_address AS (
    SELECT
        nk_customer_id,
        CONCAT(street_name, ' ', street_number) AS address,
        city,
        country_name AS country,
        address_status
    FROM stg_customer c
    INNER JOIN stg_customer_address ca
        ON c.nk_customer_id = ca.customer_id
    INNER JOIN stg_address a
        ON ca.address_id = a.address_id
    INNER JOIN stg_address_status ast
        ON ca.status_id = ast.status_id
    INNER JOIN stg_country co
        ON a.country_id = co.country_id
),

final_customer AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["nk_customer_id"]) }} AS sk_customer_id,
        *,
        {{ dbt_date.now() }} AS created_at,
        {{ dbt_date.now() }} AS updated_at
    FROM stg_customer
    INNER JOIN complete_customer_address
        USING (nk_customer_id)
)

SELECT * FROM final_customer