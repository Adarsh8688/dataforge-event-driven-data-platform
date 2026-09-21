{{ config(materialized='table') }}

select
    o.order_id,
    o.customer_key,
    d.date_key as order_date_key,
    o.order_status,
    coalesce(i.item_count, o.item_count, 0) as item_count,
    coalesce(i.product_count, o.product_count, 0) as product_count,
    coalesce(i.merchandise_value, o.merchandise_value, 0) as merchandise_value,
    coalesce(i.freight_value, o.freight_value, 0) as freight_value,
    coalesce(i.order_item_total_value, o.order_total_value, 0) as order_total_value,
    o.payment_count,
    o.payment_total_value,
    o.delivery_days,
    o.estimated_delivery_days,
    o.delivery_delay_days,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    o.loaded_at
from {{ ref('stg_fact_orders') }} o
left join {{ source('analytics', 'dim_date') }} d
    on cast(o.order_purchase_timestamp as date) = d.full_date
left join {{ ref('fct_order_item_summary') }} i
    on o.order_id = i.order_id
