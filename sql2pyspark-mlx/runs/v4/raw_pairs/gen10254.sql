select s.supplier_name, p.product_name, p.price
from suppliers s
left join products p on p.supplier_id = s.id
where p.brand is not null and p.cost <= 670;
