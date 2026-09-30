select
    s.store_name,
    s.city,
    s.sqft,
    i.last_restock,
    i.store_id
from stores s
left outer join inventory i on i.store_id = s.id
where i.store_id in (3, 6)
order by i.last_restock desc;
