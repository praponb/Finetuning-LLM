select p.category, p.price, oi.unit_price, oi.product_id from products p left join order_items oi on oi.product_id = p.id where p.cost < 500 order by p.price desc;
