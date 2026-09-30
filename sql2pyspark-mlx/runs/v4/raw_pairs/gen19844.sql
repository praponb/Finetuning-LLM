with filtered_order_items as (
    select *
    from order_items
    where order_id in (1, 23, 31)
),
summary as (
    select product_id, min(quantity) as min_quantity, avg(unit_price) as avg_unit_price
    from filtered_order_items
    group by product_id
)
select product_id, min_quantity, avg_unit_price
from summary
where min_quantity > 6.1
order by avg_unit_price;
