SELECT *, ROUND(cost / price, 2) AS margin
FROM products
WHERE (brand IN ('Globex', 'Initech') AND supplier_id IS NULL) OR launch_date >= '2025-04-01'
ORDER BY stock_qty DESC, price DESC
