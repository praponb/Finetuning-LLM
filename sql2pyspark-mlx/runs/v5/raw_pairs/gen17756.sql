select oi.id, oi.order_id, oi.quantity, oi.product_id, p.price from order_items oi full outer join products p on oi.product_id = p.id order by oi.id desc;
