{{ config(materialized='view') }}

select
    order_id,
    order_item_id,
    product_key,
    order_date_key,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    loaded_at
from {{ source('analytics', 'fact_order_items') }}
