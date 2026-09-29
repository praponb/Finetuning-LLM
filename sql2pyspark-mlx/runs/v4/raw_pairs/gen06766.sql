select p.id, p.stock_qty
from products p
where exists (select 1 from order_items oi where oi.product_id = p.id and oi.order_id in (14, 20, 29, 36));
