select oi.id, oi.unit_price, o.discount, p.category from order_items oi join orders o on oi.order_id = o.id join products p on oi.product_id = p.id where o.status = 'COMPLETED';
