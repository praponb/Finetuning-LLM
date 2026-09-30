select p.category, count(distinct oi.quantity) as unique_products, sum(oi.quantity * oi.unit_price) as revenue
from order_items oi
left join products p on oi.product_id = p.id
group by p.category
having count(distinct oi.quantity) >= 3
