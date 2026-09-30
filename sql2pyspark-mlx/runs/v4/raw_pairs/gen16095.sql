with product_totals as (
    select product_id, sum(quantity) as total_quantity, avg(quantity) as avg_quantity
    from inventory
    group by product_id
)
select
    products.cost,
    products.stock_qty,
    product_totals.total_quantity,
    product_totals.avg_quantity
from products
left join product_totals on products.id = product_totals.product_id
order by product_totals.avg_quantity desc, total_quantity;
