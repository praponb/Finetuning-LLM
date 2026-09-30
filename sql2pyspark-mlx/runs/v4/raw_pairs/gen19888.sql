select
    oi.id,
    oi.quantity,
    p.category,
    p.stock_qty
from order_items oi
inner join products p on p.id = oi.product_id
where p.price > 1120 and array_contains(p.tags, 'bestseller')
order by quantity desc;
