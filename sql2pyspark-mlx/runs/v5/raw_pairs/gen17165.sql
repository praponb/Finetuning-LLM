select p.stock_qty, p.category, i.product_id from products p left join inventory i on i.product_id = p.id where i.product_id = 1 and i.store_id = 5;
