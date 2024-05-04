{% snapshot dim_book_snapshot %}

{{
    config(
        target_schema="dwh_snapshots",
        unique_key="sk_book_id",
        strategy="timestamp",
        updated_at="updated_at"
    )
}}

SELECT *
FROM {{ ref("dim_book") }}

{% endsnapshot %}