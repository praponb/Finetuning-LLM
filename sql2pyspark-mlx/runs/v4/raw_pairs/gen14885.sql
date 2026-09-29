with filtered as (
    select
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        coalesce(unit_price, 0) as unit_price_filled
    from order_items
    where product_id is not null
      and unit_price >= 370
)
select *
from filtered
qualify row_number() over (partition by order_id order by quantity desc, id) = 1;
