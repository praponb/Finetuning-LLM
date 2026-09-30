select
    p.cost,
    s.city,
    s.rating,
    s.country
from products p
left outer join suppliers s on p.supplier_id = s.id
where p.cost < 640;
