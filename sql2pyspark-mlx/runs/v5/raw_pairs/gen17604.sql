SELECT CASE WHEN s.rating >= 3.8 THEN 'high' WHEN s.rating >= 2.5 THEN 'mid' ELSE 'low' END AS rating_bucket, SUM(CASE WHEN p.category = 'Books' THEN p.price ELSE 0 END) AS books_price, MAX(p.launch_date) AS last_launch_date
FROM products p
LEFT JOIN suppliers s ON p.supplier_id = s.id
WHERE p.stock_qty BETWEEN 118 AND 225
GROUP BY CASE WHEN s.rating >= 3.8 THEN 'high' WHEN s.rating >= 2.5 THEN 'mid' ELSE 'low' END
