SELECT p.category, MIN(i.quantity) min_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
FULL OUTER JOIN stores s ON i.store_id = s.id
WHERE UPPER(TRIM(p.product_name)) = 'NOISE-CANCEL HEADPHONES'
GROUP BY p.category
ORDER BY category DESC;
