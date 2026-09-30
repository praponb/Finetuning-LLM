select s.id, s.city
from suppliers s
where exists (select 1 from products p where p.supplier_id = s.id)
order by 1 desc;
