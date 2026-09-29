SELECT i.id, i.product_id, s.opened_date
FROM inventory i
RIGHT OUTER JOIN stores s ON s.id = i.store_id
ORDER BY id
