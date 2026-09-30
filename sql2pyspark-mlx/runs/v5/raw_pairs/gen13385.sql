select
    i.product_id,
    i.store_id,
    p.price,
    p.stock_qty,
    s.sqft
from inventory i
left join products p on i.product_id = p.id
join stores s on s.id = i.store_id
where i.store_id = 4 and p.stock_qty <= 173
order by i.product_id desc
