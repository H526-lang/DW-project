{{ config(materialized='view', schema='staging') }}

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ source('raw', 'Sellers') }}