WITH recent_products AS (
    SELECT *, UPPER(product_name) AS product_name_upper
    FROM products
    WHERE brand = 'Umbrella'
),
agg AS (
    SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, supplier_id, MIN(stock_qty) AS min_stock_qty
    FROM recent_products
    GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM'), supplier_id
)
SELECT launch_month, supplier_id, min_stock_qty
FROM agg
ORDER BY supplier_id DESC, min_stock_qty DESC
