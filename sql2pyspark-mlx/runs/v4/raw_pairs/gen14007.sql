select
    p.brand,
    p.stock_qty,
    t.quantity_range,
    t.avg_quantity
from products p
left join (select product_id, max(quantity) - min(quantity) quantity_range, avg(quantity) avg_quantity
    from order_items
    where quantity < 6
    group by product_id) t on p.id = t.product_id
where t.quantity_range > 67
order by brand desc;
