WITH supplier_stats AS (
    SELECT supplier_id, MAX(launch_date) AS last_launch_date, MAX(stock_qty) AS max_stock_qty
    FROM products
    WHERE cost IS NULL
    GROUP BY supplier_id
)
SELECT
    s.id,
    s.supplier_name,
    ss.last_launch_date,
    ss.max_stock_qty
FROM suppliers s
LEFT JOIN supplier_stats ss ON s.id = ss.supplier_id
WHERE ss.max_stock_qty > 210
ORDER BY id
