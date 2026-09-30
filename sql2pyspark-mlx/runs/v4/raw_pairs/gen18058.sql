with base as (
    select *
    from order_items
    where unit_price is null
),
order_id_summary as (
    select order_id, max(unit_price) - min(unit_price) as unit_price_range
    from base
    group by order_id
)
select *
from order_id_summary
order by unit_price_range
