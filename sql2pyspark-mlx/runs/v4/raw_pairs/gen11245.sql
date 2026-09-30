SELECT category, SUM(CASE WHEN brand = 'Initech' THEN price ELSE 0 END) AS initech_price, ROUND((100 * SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_acme
FROM products
WHERE category IN ('Sports', 'Fashion') AND (price <= 1050 OR brand <> 'Acme')
GROUP BY category
ORDER BY initech_price;
