select
    i.id,
    i.product_id,
    i.store_id,
    i.quantity,
    s.opened_date
from inventory i
join stores s on i.store_id = s.id
where i.last_restock <= '2023-07-01'
