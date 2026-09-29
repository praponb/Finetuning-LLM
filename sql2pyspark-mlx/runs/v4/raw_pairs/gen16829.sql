select
    p.category,
    p.brand,
    min(oi.quantity) as min_quantity,
    max(oi.quantity) as max_quantity
from order_items oi
join products p on p.id = oi.product_id
group by p.category, p.brand
order by max_quantity asc, p.category asc;
