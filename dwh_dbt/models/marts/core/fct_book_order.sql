{{ config(schema='final') }}

WITH stg_book_order AS (
    SELECT
        order_id AS nk_order_id,
        order_date::date AS order_date,
        customer_id AS nk_customer_id,
        shipping_method_id,
        dest_address_id
    FROM {{ ref("stg_dwh_cust_order") }}
),

stg_shipping_method AS (
    SELECT *
    FROM {{ ref("stg_dwh_shipping_method") }}
),

stg_address AS (
    SELECT *
    FROM {{ ref("stg_dwh_address") }}
),

stg_country AS (
    SELECT *
    FROM {{ ref("stg_dwh_country") }}
),

stg_order_line AS (
    SELECT *
    FROM {{ ref("stg_dwh_order_line") }}
),

stg_order_history AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY status_date DESC) AS row_num
    FROM {{ ref("stg_dwh_order_history") }}
),

latest_order_history AS (
    SELECT
        order_id,
        status_id,
        status_date
    FROM stg_order_history
    WHERE row_num = 1
),

stg_order_status AS (
    SELECT *
    FROM {{ ref("stg_dwh_order_status") }}
),

dim_book AS (
    SELECT *
    FROM {{ ref("dim_book") }}
),

dim_customer AS (
    SELECT *
    FROM {{ ref("dim_customer") }}
),

dim_date AS (
    SELECT *
    FROM {{ ref("dim_date") }}
),

final_book_order AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["nk_order_id"]) }} AS sk_order_id,
        nk_order_id,
        dd.date_day AS order_date,
        dc.sk_customer_id,
        db.sk_book_id,
        ol.price,
        sm.method_name AS shipping_method,
        sm.cost AS shipping_cost,
        os.status_value AS status,
        oh.status_date,
        CONCAT(a.street_name, ' ', a.street_number) AS dest_address,
        a.city AS dest_city,
        co.country_name AS dest_country,
        {{ dbt_date.now() }} AS created_at,
        {{ dbt_date.now() }} AS updated_at
    FROM stg_book_order bo
    INNER JOIN dim_date dd
        ON bo.order_date = dd.date_day
    INNER JOIN dim_customer dc
        ON bo.nk_customer_id = dc.nk_customer_id
    INNER JOIN stg_order_line ol
        ON bo.nk_order_id = ol.order_id
    INNER JOIN dim_book db
        ON ol.book_id = db.nk_book_id
    INNER JOIN stg_shipping_method sm
        ON bo.shipping_method_id = sm.method_id
    INNER JOIN latest_order_history oh
        ON bo.nk_order_id = oh.order_id
    INNER JOIN stg_order_status os
        ON oh.status_id = os.status_id
    INNER JOIN stg_address a
        ON bo.dest_address_id = a.address_id
    INNER JOIN stg_country co
        ON a.country_id = co.country_id
)

SELECT * FROM final_book_order