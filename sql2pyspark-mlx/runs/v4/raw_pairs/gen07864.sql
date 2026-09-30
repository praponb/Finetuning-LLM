select p.category, count(distinct i.quantity) as unique_stores, count(*) as num_inventory
from inventory i
full outer join products p on p.id = i.product_id
inner join suppliers s on p.supplier_id = s.id
group by p.category
having count(*) >= 5
