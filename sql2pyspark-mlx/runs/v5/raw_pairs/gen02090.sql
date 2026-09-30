select o.store_id, avg(oi.quantity) as avg_quantity
from order_items oi
left join orders o on oi.order_id = o.id
full outer join products p on oi.product_id = p.id
group by o.store_id
having avg_quantity > 3.9
order by store_id desc, avg_quantity desc;
