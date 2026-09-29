select
    i.quantity,
    i.last_restock,
    i.product_id,
    s.region,
    s.opened_date,
    date_sub(s.opened_date, 29) as opened_date_minus_29d
from inventory i
inner join stores s on i.store_id = s.id
order by opened_date_minus_29d, quantity desc;
