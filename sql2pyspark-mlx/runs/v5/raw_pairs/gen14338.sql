SELECT brand, COUNT(*) AS num_products, COUNT(price) AS price_count
FROM products
WHERE supplier_id IS NOT NULL
GROUP BY brand
HAVING COUNT(*) >= 5
ORDER BY num_products DESC, 1
