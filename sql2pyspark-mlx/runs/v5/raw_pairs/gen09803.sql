with ranked as (
    -- Keep one row per key (latest first)
    select *
    from order_items
    qualify row_number() over (partition by product_id order by unit_price nulls last, id) = 1
)
select
    id,
    order_id,
    quantity
from ranked
order by order_id, id;
