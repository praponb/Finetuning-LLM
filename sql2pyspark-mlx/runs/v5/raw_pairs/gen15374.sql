select s.id, s.country, s.rating
from suppliers s
where not exists (select 1 from products p where p.supplier_id = s.id and p.price < 1100)
