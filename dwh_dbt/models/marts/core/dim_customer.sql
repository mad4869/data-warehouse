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

final_customer AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["nk_customer_id"]) }} AS sk_customer_id,
        nk_customer_id,
        first_name,
        last_name,
        full_name,
        email,
        {{ dbt_date.now() }} AS created_at,
        {{ dbt_date.now() }} AS updated_at
    FROM stg_customer c
)

SELECT * FROM final_customer