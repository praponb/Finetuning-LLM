select oi.order_id, p.category
from order_items oi
inner join products p on oi.product_id = p.id
order by 2 desc
