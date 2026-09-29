WITH recent_products AS (
    SELECT *
    FROM products
    WHERE LENGTH(product_name) > 5
),
agg AS (
    SELECT QUARTER(launch_date) AS launch_quarter, MIN(stock_qty) AS min_stock_qty
    FROM recent_products
    GROUP BY QUARTER(launch_date)
)
SELECT launch_quarter, min_stock_qty
FROM agg
ORDER BY min_stock_qty
