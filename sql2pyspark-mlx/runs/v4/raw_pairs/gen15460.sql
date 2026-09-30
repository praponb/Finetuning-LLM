select p.price, p.brand, oi.product_id from products p left join order_items oi on oi.product_id = p.id where oi.quantity != 8 order by p.price asc;
