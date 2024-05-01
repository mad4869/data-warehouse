{{ config(
    schema='staging',
    alias='stg_shipping_method'
    ) 
}}

SELECT *
FROM {{ source('pacbook', 'shipping_method') }}