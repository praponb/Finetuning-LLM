SELECT CASE WHEN stock_qty >= 347 THEN 'high' WHEN stock_qty >= 113 THEN 'mid' ELSE 'low' END AS stock_qty_bucket, COUNT(*) AS cnt FROM products GROUP BY 1 HAVING COUNT(*) > 5
