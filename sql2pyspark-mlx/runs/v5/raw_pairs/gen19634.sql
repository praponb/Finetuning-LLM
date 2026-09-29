select p.id, p.brand
from products p
where p.supplier_id in (select id from suppliers where country != 'US')
order by brand;
