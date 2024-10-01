{{ config(materialized='table', schema='analytics') }}

SELECT
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    g.geolocation_lat,
    g.geolocation_lng
FROM {{ ref('Seller') }} s
LEFT JOIN {{ ref('geolocation') }} g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
