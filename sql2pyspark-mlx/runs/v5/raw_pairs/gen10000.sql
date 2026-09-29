select p.category, p.brand, sum(oi.quantity) as total_quantity from order_items oi inner join orders o on o.id = oi.order_id inner join products p on p.id = oi.product_id group by p.category, p.brand
