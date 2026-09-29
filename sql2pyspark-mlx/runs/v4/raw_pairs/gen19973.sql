SELECT brand, SUM(price) AS total_price, ROUND(SUM(price) * 100.0 / SUM(SUM(price)) OVER (), 2) AS pct_of_total FROM products GROUP BY brand ORDER BY brand, 3 LIMIT 3
