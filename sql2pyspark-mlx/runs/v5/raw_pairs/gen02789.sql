select
    i.id,
    i.last_restock,
    i.product_id,
    i.quantity,
    s.city,
    year(s.opened_date) as opened_date_year
from inventory i
join stores s on s.id = i.store_id
where i.store_id in (1, 4, 5, 6);
