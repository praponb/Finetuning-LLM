select
    s.id,
    s.rating,
    s.supplier_name,
    s.country
from suppliers s
where s.rating < 3.0 and not exists (select 1 from products p where p.supplier_id = s.id and p.price >= 1150)
