SELECT
    CASE WHEN p.price >= 1010 THEN 'high' WHEN p.price >= 460 THEN 'mid' ELSE 'low' END AS price_bucket,
    CASE WHEN p.cost >= 580 THEN 'high' WHEN p.cost >= 210 THEN 'mid' ELSE 'low' END AS cost_bucket,
    COUNT(DISTINCT i.quantity) AS unique_products,
    ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
JOIN products p ON i.product_id = p.id
WHERE i.store_id IN (2, 3, 4, 5)
GROUP BY CASE WHEN p.price >= 1010 THEN 'high' WHEN p.price >= 460 THEN 'mid' ELSE 'low' END, CASE WHEN p.cost >= 580 THEN 'high' WHEN p.cost >= 210 THEN 'mid' ELSE 'low' END;
