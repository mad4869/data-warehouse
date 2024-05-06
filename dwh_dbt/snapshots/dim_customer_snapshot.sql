{% snapshot dim_customer_snapshot %}

{{
    config(
        target_schema="dwh_snapshot",
        unique_key="sk_customer_id",
        strategy="timestamp",
        updated_at="updated_at"
    )
}}

SELECT *
FROM {{ ref("dim_customer") }}

{% endsnapshot %}