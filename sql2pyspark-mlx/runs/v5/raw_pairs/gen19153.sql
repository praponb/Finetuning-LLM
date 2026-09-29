select
    p.launch_date,
    p.price,
    p.supplier_id,
    s.country,
    s.rating
from products p
left join suppliers s on p.supplier_id = s.id;
