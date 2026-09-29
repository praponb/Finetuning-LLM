SELECT
    s.id,
    s.rating,
    s.country,
    t.price_count
FROM suppliers s
LEFT JOIN (SELECT supplier_id, COUNT(price) AS price_count
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.price_count > 4;
