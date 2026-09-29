WITH base AS (
    SELECT *
    FROM products
    WHERE stock_qty < 252 AND stock_qty >= 191
),
Summary AS (
    SELECT
        DATE_TRUNC('MONTH', launch_date) AS launch_month,
        category,
        SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
        MIN(stock_qty) AS min_stock_qty
    FROM base
    GROUP BY DATE_TRUNC('MONTH', launch_date), category
)
SELECT
    launch_month,
    category,
    brand_list,
    min_stock_qty
FROM Summary;
