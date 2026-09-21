{{ config(materialized='table') }}

select
    order_id,
    count(*) as item_count,
    count(distinct product_key) as product_count,
    sum(price) as merchandise_value,
    sum(freight_value) as freight_value,
    sum(price + freight_value) as order_item_total_value
from {{ ref('stg_fact_order_items') }}
group by order_id
