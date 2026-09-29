with product_totals as (
    select product_id, count(distinct quantity) unique_orders, max(quantity) max_quantity
    from order_items
    group by product_id
)
select
    p.id,
    p.launch_date,
    p.product_name,
    pt.unique_orders,
    pt.max_quantity
from products p
inner join product_totals pt on p.id = pt.product_id
where pt.max_quantity >= 3.8;
