with valid as (
    -- Basic cleansing and defaults
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        cast(unit_price as int) as unit_price_int,
        ceil(unit_price) as unit_price_whole
    from order_items
    where product_id is not null
      and quantity >= 3
)
select order_id, product_id
from valid
intersect
select order_id, product_id
from order_items
where order_id = 22
