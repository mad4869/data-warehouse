{% snapshot fct_repeat_time_order_snapshot %}

{{
    config(
        target_schema="dwh_snapshot",
        unique_key="sk_repeat_order_time_id",
        strategy="timestamp",
        updated_at="updated_at"
    )
}}

SELECT *
FROM {{ ref("fct_repeat_order_time") }}

{% endsnapshot %}