{% macro generate_nlp_data(target_schema='ml') %}
  
  {% set table_name = target_schema ~ '.nlp_data' %}

  {% set sql %}
  CREATE OR REPLACE TABLE {{ table_name }} AS
  WITH review_comments AS (
      SELECT
          review_id,
          order_id,
          review_comment_message,
          review_comment_title,
          review_creation_date,
          review_answer_timestamp
      FROM {{ ref('Order_Reviews') }}
      WHERE review_comment_message IS NOT NULL
  ),
  
  review_metadata AS (
      SELECT
          r.review_id,
          r.order_id,
          o.customer_id,
          o.order_status,
          c.customer_city,
          c.customer_state,
          r.review_score,
          r.review_creation_date,
          r.review_answer_timestamp
      FROM {{ ref('Order_Reviews') }} r
      LEFT JOIN {{ ref('Orders') }} o ON r.order_id = o.order_id
      LEFT JOIN {{ ref('Customers') }} c ON o.customer_id = c.customer_id
  )
  
  SELECT
      rc.review_id,
      rc.order_id,
      rm.customer_id,
      rm.order_status,
      rm.customer_city,
      rm.customer_state,
      rm.review_score,
      rc.review_comment_message,
      rc.review_comment_title,
      rc.review_creation_date,
      rc.review_answer_timestamp
  FROM review_comments rc
  JOIN review_metadata rm ON rc.review_id = rm.review_id;
  {% endset %}

  {% do run_query(sql) %}

{% endmacro %}
