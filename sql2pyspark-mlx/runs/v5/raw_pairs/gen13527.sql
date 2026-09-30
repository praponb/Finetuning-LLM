select oi.id, oi.product_id, oi.unit_price
from order_items oi
where oi.product_id in (select id from products where brand = 'Initech');
