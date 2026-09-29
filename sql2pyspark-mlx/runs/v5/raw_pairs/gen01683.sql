select
    i.last_restock,
    i.store_id,
    i.product_id,
    s.city,
    s.store_name
from inventory i
inner join stores s on i.store_id = s.id
order by product_id desc;
