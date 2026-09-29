WITH filtered AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        cost / NULLIF(stock_qty, 0) AS cost_per_stock_qty
    FROM products
    WHERE MONTH(launch_date) IN (4, 7, 8)
)
SELECT
    p.country,
    e.category,
    COUNT(*) AS cnt,
    AVG(e.cost_per_stock_qty) AS avg_cost_per_stock_qty,
    MAX(e.price) AS max_price,
    GROUPING_ID() AS gid
FROM filtered e
LEFT JOIN suppliers p ON e.supplier_id = p.id
GROUP BY ROLLUP (p.country, e.category);
