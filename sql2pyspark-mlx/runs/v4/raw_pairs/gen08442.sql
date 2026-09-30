select
    i.id,
    i.store_id,
    i.product_id,
    s.opened_date,
    s.sqft,
    s.store_name
from inventory i
left outer join stores s on i.store_id = s.id
where s.sqft != 9203
