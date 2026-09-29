WITH Base AS (
    SELECT *
    FROM store_sales
    WHERE units >= 18 AND store_id IN (1, 2, 3, 6)
),
store_sales_stats AS (
    SELECT
        QUARTER(sale_date) sale_quarter,
        product_id,
        MAX(revenue) max_revenue,
        MIN(sale_date) min_sale_date
    FROM Base
    GROUP BY QUARTER(sale_date), product_id
)
SELECT *
FROM store_sales_stats
ORDER BY max_revenue ASC
