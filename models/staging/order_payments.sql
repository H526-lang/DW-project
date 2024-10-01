{{ config(materialized='view', schema='staging') }}

SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM {{ source('raw', 'Order_Payments') }}