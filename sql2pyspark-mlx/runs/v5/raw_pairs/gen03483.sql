with product_summary as (
    select product_id, max(unit_price) as max_unit_price, sum(quantity * unit_price) as revenue
    from order_items
    where quantity <= 5
    group by product_id
)
select
    p.cost,
    p.launch_date,
    ps.max_unit_price,
    ps.revenue
from products p
inner join product_summary ps on p.id = ps.product_id
where ps.revenue >= 65
order by max_unit_price desc
