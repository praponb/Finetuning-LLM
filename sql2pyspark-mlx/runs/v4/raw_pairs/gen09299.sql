select
    i.product_id,
    s.sqft,
    s.region,
    p.supplier_id,
    p.launch_date
from inventory i
inner join stores s on i.store_id = s.id
right join products p on i.product_id = p.id
where p.stock_qty between 121 and 306 and p.brand <> 'Umbrella'
