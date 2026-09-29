select
    s.id,
    s.store_name,
    s.sqft,
    s.city
from stores s
where s.city in ('Singapore', 'Chiang Mai') and s.id in (select store_id from orders where discount <= 0.23)
