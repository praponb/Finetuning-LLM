select p.id, p.price, p.supplier_id
from products p
where p.supplier_id in (select id from suppliers where supplier_name like '%Imp%')
order by supplier_id desc
