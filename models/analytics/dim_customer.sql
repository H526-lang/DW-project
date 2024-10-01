{{ config(materialized='table', schema='analytics') }}

SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    g.geolocation_lat,
    g.geolocation_lng
FROM {{ ref('Customer') }} c
LEFT JOIN {{ ref('geolocation') }} g
    ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix