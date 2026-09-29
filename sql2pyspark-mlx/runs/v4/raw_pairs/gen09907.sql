with base as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        cast(quantity as double) as quantity_str
    from order_items
    where order_id in (28, 35)
)
-- QUALIFY-style window filter
select *, row_number() over (partition by order_id order by quantity desc, id) as rank_in_group
from base
qualify rank_in_group = 1
