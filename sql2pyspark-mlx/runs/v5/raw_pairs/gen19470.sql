SELECT CASE WHEN p.stock_qty >= 364 THEN 'high' WHEN p.stock_qty >= 52 THEN 'mid' ELSE 'low' END AS stock_qty_bucket, MAX(ss.units) - MIN(ss.units) AS units_range
FROM store_sales ss
INNER JOIN products p ON p.id = ss.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN p.stock_qty >= 364 THEN 'high' WHEN p.stock_qty >= 52 THEN 'mid' ELSE 'low' END
ORDER BY 1 ASC, 2 ASC
LIMIT 1
