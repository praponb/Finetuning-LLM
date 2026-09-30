SELECT
    i.product_id,
    i.store_id,
    p.cost,
    p.price,
    s.rating,
    DATE_FORMAT(i.last_restock, 'yyyy-MM') AS last_restock_label
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.rating < 3.1
ORDER BY price DESC
