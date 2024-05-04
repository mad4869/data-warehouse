{% snapshot fct_book_order_snapshot %}

{{
    config(
        target_schema="dwh_snapshots",
        unique_key="order_book_id",
        strategy="timestamp",
        updated_at="updated_at"
    )
}}

SELECT 
    sk_order_id || '-' || sk_book_id AS order_book_id,
    *
FROM {{ ref("fct_book_order") }}

{% endsnapshot %}