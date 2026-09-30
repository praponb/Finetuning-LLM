SELECT
    ss.store_id,
    s.opened_date,
    p.stock_qty,
    DATE_TRUNC('year', s.opened_date) AS opened_date_period
FROM store_sales ss
JOIN stores s ON s.id = ss.store_id
FULL OUTER JOIN products p ON ss.product_id = p.id
WHERE p.supplier_id IN (5, 15, 29, 36) AND ss.revenue < 1900
