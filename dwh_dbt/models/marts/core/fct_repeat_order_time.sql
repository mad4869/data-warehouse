{{ config(schema='final') }}

WITH stg_repeat_order_time AS (
    SELECT
        nk_customer_id,
        nk_order_id,
        order_date,
        previous_order_date,
        (order_date - previous_order_date) AS order_interval
    FROM (
        SELECT
            customer_id AS nk_customer_id,
            order_id AS nk_order_id,
            CAST(order_date AS TIMESTAMP),
            CAST(LAG(order_date) OVER(PARTITION BY customer_id ORDER BY order_date ASC) AS TIMESTAMP) AS previous_order_date
        FROM {{ ref("stg_dwh_cust_order") }}
    ) AS stg_cust_order
),

dim_customer AS (
    SELECT *
    FROM {{ ref("dim_customer") }}
),

final_repeat_order_time AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["nk_order_id"]) }} AS sk_repeat_order_time_id,
        dc.sk_customer_id,
        rot.nk_customer_id,
        rot.nk_order_id,
        rot.order_date,
        rot.previous_order_date,
        rot.order_interval,
        {{ dbt_date.now() }} AS created_at,
        {{ dbt_date.now() }} AS updated_at
    FROM stg_repeat_order_time rot
    INNER JOIN dim_customer dc
        USING(nk_customer_id)
)

SELECT * FROM final_repeat_order_time