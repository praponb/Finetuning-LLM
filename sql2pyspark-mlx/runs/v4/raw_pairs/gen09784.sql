with ProductStats as (
    select product_id, round(avg(unit_price), 2) as avg_unit_price, sum(unit_price) as total_unit_price
    from order_items
    group by product_id
)
select p.stock_qty, P.avg_unit_price, P.total_unit_price
from products p
join ProductStats P on p.id = P.product_id
order by avg_unit_price desc
