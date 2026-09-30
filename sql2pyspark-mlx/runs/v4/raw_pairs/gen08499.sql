select
    i.id,
    i.quantity,
    s.sqft,
    p.launch_date
from inventory i
join stores s on i.store_id = s.id
join products p on i.product_id = p.id
where i.store_id = 1
