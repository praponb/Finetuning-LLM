WITH supplier_summary AS (
    SELECT supplier_id, MAX(stock_qty) - MIN(stock_qty) stock_qty_range
    FROM products
    WHERE supplier_id = 9
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.country,
    s.rating,
    ss.stock_qty_range
FROM suppliers s
LEFT OUTER JOIN supplier_summary ss ON s.id = ss.supplier_id
WHERE ss.stock_qty_range >= 39;
