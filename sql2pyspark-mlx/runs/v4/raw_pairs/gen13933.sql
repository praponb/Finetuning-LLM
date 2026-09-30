SELECT
    s.supplier_name,
    s.rating,
    t.acme_cost,
    t.acme_price
FROM suppliers s
INNER JOIN (SELECT supplier_id, SUM(CASE WHEN brand = 'Acme' THEN cost ELSE 0 END) acme_cost, SUM(CASE WHEN brand = 'Acme' THEN price ELSE 0 END) acme_price
    FROM products
    WHERE SIZE(tags) > 1
    GROUP BY supplier_id) t ON s.id = t.supplier_id
ORDER BY supplier_name
