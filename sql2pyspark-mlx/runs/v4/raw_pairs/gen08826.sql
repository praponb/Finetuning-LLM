select
    p.id,
    p.brand,
    p.category,
    p.cost,
    s.country,
    s.rating
from products p
join suppliers s on p.supplier_id = s.id
where p.supplier_id is null
order by s.rating desc
