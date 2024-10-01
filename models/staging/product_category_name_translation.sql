{{ config(materialized='view', schema='staging') }}

SELECT
    product_category_name,
    product_category_name_english
FROM {{ source('raw', 'Product_Category_Name_Translation') }}