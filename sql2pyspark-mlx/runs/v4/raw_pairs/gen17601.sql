select
    s.id,
    s.rating,
    s.supplier_name,
    s.country
from suppliers s
where not exists (select 1 from products p where p.supplier_id = s.id);
