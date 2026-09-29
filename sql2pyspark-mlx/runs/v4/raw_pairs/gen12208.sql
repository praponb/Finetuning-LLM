select p.id, count(i.id) as inventory_count, count(i.quantity) as quantity_count
from products p
inner join inventory i on p.id = i.product_id
group by p.id
