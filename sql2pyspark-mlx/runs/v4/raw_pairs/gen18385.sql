select s.id, s.supplier_name
from suppliers s
where s.country not in ('SG', 'TH', 'US') and s.id in (select supplier_id from products)
order by id asc;
