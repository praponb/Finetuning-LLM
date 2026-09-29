select p.brand, count(*) as row_count from order_items oi left outer join products p on oi.product_id = p.id join orders o on oi.order_id = o.id group by p.brand;
