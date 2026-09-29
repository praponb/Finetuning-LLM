select
    p.id,
    p.stock_qty,
    p.cost,
    t.total_revenue
from products p
inner join (select product_id, sum(revenue) as total_revenue
    from store_sales
    where units < 30
    group by product_id) t on p.id = t.product_id
where t.total_revenue >= 7000;
