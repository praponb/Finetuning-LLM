SELECT p.cost, i.store_id
FROM products p
LEFT JOIN inventory i ON i.product_id = p.id
ORDER BY i.store_id, p.cost;
