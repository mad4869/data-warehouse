{% snapshot fct_monthly_order_snapshot %}

{{
    config(
        target_schema="dwh_snapshot",
        unique_key="sk_monthly_order_id",
        strategy="timestamp",
        updated_at="updated_at"
    )
}}

SELECT *
FROM {{ ref("fct_monthly_order") }}

{% endsnapshot %}