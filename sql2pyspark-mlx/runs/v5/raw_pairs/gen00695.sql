SELECT s.id, s.country, t.min_price
FROM suppliers s
INNER JOIN (SELECT supplier_id, MIN(price) min_price
    FROM products
    GROUP BY supplier_id) t ON s.id = t.supplier_id
WHERE t.min_price > 840;
