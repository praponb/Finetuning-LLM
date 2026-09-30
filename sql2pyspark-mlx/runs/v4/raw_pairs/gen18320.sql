SELECT
    *,
    ROUND(revenue / SUM(revenue) OVER (PARTITION BY store_id), 4) AS revenue_share,
    LEAD(revenue, 2, 0) OVER w_recent AS next_revenue
FROM store_sales
WINDOW w_recent AS (ORDER BY sale_date, id)
