SELECT i.store_id, p.price, s.region
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
FULL OUTER JOIN stores s ON s.id = i.store_id;
