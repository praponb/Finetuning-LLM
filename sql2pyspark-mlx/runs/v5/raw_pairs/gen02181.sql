SELECT s.id, s.city, t.quantity_range
FROM stores s
INNER JOIN (SELECT store_id, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM inventory
    GROUP BY store_id) t ON s.id = t.store_id;
