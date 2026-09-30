WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_id,
        sale_date,
        units,
        revenue,
        units + revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > DATE '2023-08-21'
      AND revenue >= 1000
)
-- Running and moving aggregates
SELECT
    *,
    AVG(revenue) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS revenue_avg_3_prior,
    LAG(revenue, 2, 0) OVER (PARTITION BY product_id ORDER BY revenue, id) AS prev_revenue
FROM cleaned;
