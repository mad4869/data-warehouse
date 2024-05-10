{{ config(schema='final') }}

WITH stg_monthly_order AS (
    SELECT
        EXTRACT(MONTH FROM order_date::date) AS month,
        EXTRACT(YEAR FROM order_date::date) AS year,
        order_id as nk_order_id
    FROM {{ ref("stg_dwh_cust_order") }}
),

stg_order_line AS (
    SELECT *
    FROM {{ ref("stg_dwh_order_line") }}
),

dim_book AS (
    SELECT *
    FROM {{ ref("dim_book") }}
),

final_monthly_order AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["month", "year", "sk_book_id"]) }} AS sk_monthly_order_id,
        month,
        year,
        db.sk_book_id,
        COUNT(mo.nk_order_id) AS total_order,
        {{ dbt_date.now() }} AS created_at,
        {{ dbt_date.now() }} AS updated_at
    FROM
        stg_monthly_order mo
    INNER JOIN stg_order_line ol
        ON mo.nk_order_id = ol.order_id
    INNER JOIN dim_book db
        ON ol.book_id = db.nk_book_id
    GROUP BY
        month, year, db.sk_book_id
    ORDER BY
        year DESC,
        month ASC
)

SELECT * FROM final_monthly_order