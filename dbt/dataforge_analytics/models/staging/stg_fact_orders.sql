{{ config(materialized='view') }}

select
    order_id,
    customer_key,
    order_date_key,
    order_status,
    item_count,
    product_count,
    merchandise_value,
    freight_value,
    order_total_value,
    payment_count,
    payment_total_value,
    delivery_days,
    estimated_delivery_days,
    delivery_delay_days,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    loaded_at
from {{ source('analytics', 'fact_orders') }}
