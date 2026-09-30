SELECT
    s.region,
    CASE WHEN s.sqft >= 13714 THEN 'high' WHEN s.sqft >= 5641 THEN 'mid' ELSE 'low' END AS sqft_bucket,
    ROUND(STDDEV(i.quantity), 2) AS stddev_quantity,
    COUNT(DISTINCT i.quantity) AS unique_products
FROM inventory i
LEFT JOIN products p ON p.id = i.product_id
LEFT JOIN stores s ON s.id = i.store_id
WHERE p.stock_qty IS NOT NULL
GROUP BY s.region, CASE WHEN s.sqft >= 13714 THEN 'high' WHEN s.sqft >= 5641 THEN 'mid' ELSE 'low' END
ORDER BY 3 DESC;
