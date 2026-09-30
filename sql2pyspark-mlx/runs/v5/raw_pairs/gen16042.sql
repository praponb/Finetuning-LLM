SELECT
    id,
    product_id,
    units,
    revenue,
    MIN(revenue) OVER w_grp running_min_revenue,
    SUM(revenue) OVER w_grp running_sum_revenue,
    AVG(revenue) OVER w_grp running_avg_revenue,
    COUNT(revenue) OVER w_grp running_count_revenue,
    LAG(revenue, 1) OVER (PARTITION BY store_id ORDER BY units, id) prev_revenue
FROM store_sales
WINDOW w_grp AS (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
