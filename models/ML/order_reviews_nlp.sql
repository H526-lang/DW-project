{{ config(materialized='table', schema='ml') }}

SELECT
    r.review_id,
    r.order_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp,
    p.product_category_name,
    ct.product_category_name_english,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,
    DATEDIFF(day, o.order_purchase_timestamp, o.order_delivered_customer_date) AS delivery_time
FROM {{ ref('order_reviews') }} r
JOIN {{ ref('orders') }} o ON r.order_id = o.order_id
JOIN {{ ref('Orders_items') }} oi ON o.order_id = oi.order_id
JOIN {{ ref('product') }} p ON oi.product_id = p.product_id
LEFT JOIN {{ ref('product_category_name_translation') }} ct ON p.product_category_name = ct.product_category_name
WHERE r.review_comment_message IS NOT NULL
