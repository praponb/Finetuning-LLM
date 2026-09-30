SELECT category, SUM(price) AS total_price, ROUND((100 * SUM(CASE WHEN brand = 'Umbrella' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_umbrella
FROM products
GROUP BY category
HAVING pct_umbrella > 76;
