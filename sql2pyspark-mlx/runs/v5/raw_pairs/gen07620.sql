select s.city, s.supplier_name, p.price
from suppliers s
left join products p on p.supplier_id = s.id;
