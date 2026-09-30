WITH cleaned AS (
    SELECT
        id,
        product_name,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        ROUND(stock_qty / cost, 2) AS stock_qty_per_cost
    FROM products
    WHERE MONTH(launch_date) IN (1, 4, 12)
)
SELECT
    p.city,
    COUNT(*) AS num_rows,
    SUM(s.stock_qty) AS total_stock_qty,
    ROUND(AVG(s.stock_qty_per_cost), 2) AS avg_stock_qty_per_cost,
    GROUPING_ID() AS gid
FROM cleaned s
INNER JOIN suppliers p ON s.supplier_id = p.id
GROUP BY CUBE (p.city)
ORDER BY num_rows DESC NULLS LAST
