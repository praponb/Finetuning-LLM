select oi.id, oi.product_id
from order_items oi
where oi.product_id not in (select id from products where array_contains(tags, 'bestseller'));
