SELECT category, SUM(CASE WHEN brand = 'Umbrella' THEN stock_qty ELSE 0 END) AS umbrella_stock_qty
FROM products
GROUP BY category
ORDER BY category
