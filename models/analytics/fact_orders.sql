{{ config(materialized='table', schema='analytics') }}

SELECT
    o.order_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    DATE(o.order_purchase_timestamp) AS order_date,
    o.order_status,
    oi.price,
    oi.freight_value,
    op.payment_type,
    op.payment_value,
    DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date) AS delivery_time,
    or.review_score
FROM {{ ref('orders') }} o
JOIN {{ ref('Orders_items') }} oi ON o.order_id = oi.order_id
JOIN {{ ref('order_payments') }} op ON o.order_id = op.order_id
LEFT JOIN {{ ref('order_reviews') }} or ON o.order_id = or.order_id
