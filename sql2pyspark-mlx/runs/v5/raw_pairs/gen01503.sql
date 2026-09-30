SELECT
    CASE WHEN cost >= 780 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    DATE_TRUNC('WEEK', launch_date) AS week_start,
    COUNT(DISTINCT id) AS distinct_ids,
    CORR(stock_qty, cost) AS corr_stock_qty_cost,
    APPROX_PERCENTILE(stock_qty, 0.95) AS stock_qty_p95,
    GROUPING_ID() AS gid
FROM products
GROUP BY CUBE (CASE WHEN cost >= 780 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', launch_date))
HAVING CORR(stock_qty, cost) >= 0;
