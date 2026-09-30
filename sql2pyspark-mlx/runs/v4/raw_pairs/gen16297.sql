select p.id, p.stock_qty, s.city
from products p
left join suppliers s on p.supplier_id = s.id
where s.rating between 2.0 and 2.4
order by city, id desc;
