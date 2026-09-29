select p.price, i.store_id, i.last_restock, coalesce(p.cost, 0) as cost_filled from products p left join inventory i on i.product_id = p.id where i.product_id = 2
