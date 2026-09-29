select p.id, count(i.id) as inventory_count, max(i.quantity) - min(i.quantity) as quantity_range
from products p
left join inventory i on i.product_id = p.id
group by p.id
order by id asc;
