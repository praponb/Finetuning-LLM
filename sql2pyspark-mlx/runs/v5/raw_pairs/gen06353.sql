select p.id, p.category
from products p
where p.supplier_id not in (select id from suppliers where supplier_name like 'L%')
order by p.category desc, p.id desc
limit 1
