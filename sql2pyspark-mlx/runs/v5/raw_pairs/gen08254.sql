SELECT p.brand, MIN(ss.units) AS min_units
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON p.id = ss.product_id
WHERE p.stock_qty != 128
GROUP BY p.brand
ORDER BY min_units DESC
