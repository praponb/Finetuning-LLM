with ProductTotals as (
    select product_id, avg(unit_price) as avg_unit_price, count(distinct quantity) as unique_orders
    from order_items
    where unit_price is null
    group by product_id
)
select
    p.stock_qty,
    p.launch_date,
    P.avg_unit_price,
    P.unique_orders
from products p
left outer join ProductTotals P on p.id = P.product_id
where P.unique_orders >= 4
order by P.unique_orders
