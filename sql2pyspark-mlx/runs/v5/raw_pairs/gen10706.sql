SELECT i.store_id, p.product_name, s.city
FROM inventory i
FULL OUTER JOIN products p ON p.id = i.product_id
LEFT OUTER JOIN stores s ON i.store_id = s.id
WHERE i.last_restock < '2023-05-01'
ORDER BY city
