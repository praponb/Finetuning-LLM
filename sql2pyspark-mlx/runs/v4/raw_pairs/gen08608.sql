SELECT
    ss.product_id,
    p.stock_qty,
    s.rating,
    s.city,
    ROUND(ss.revenue * 1.15, 2) AS adjusted_revenue
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.city IN ('Singapore', 'Bangkok') AND s.rating >= 3.5
