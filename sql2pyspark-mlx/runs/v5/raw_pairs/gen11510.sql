SELECT category, brand, COUNT(*) AS product_count
FROM products
WHERE ARRAY_CONTAINS(tags, 'bestseller') AND brand <> 'Umbrella'
GROUP BY 1, 2
ORDER BY product_count DESC
