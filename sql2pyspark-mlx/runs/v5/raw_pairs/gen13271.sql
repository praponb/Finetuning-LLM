WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        shipped_date,
        delivered_date,
        cost,
        AVG(cost) OVER w_part AS running_avg_cost,
        SUM(cost) OVER w_part AS running_sum_cost,
        MAX(cost) OVER w_part AS running_max_cost,
        MIN(cost) OVER w_part AS running_min_cost
    FROM shipments
    WINDOW w_part AS (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    SUM(running_avg_cost) AS total_running_avg_cost,
    ROUND(AVG(running_sum_cost), 2) AS avg_running_sum_cost,
    ROUND(AVG(running_max_cost), 2) AS avg_running_max_cost,
    APPROX_PERCENTILE(running_min_cost, 0.95) AS running_min_cost_p95,
    ROUND(AVG(running_min_cost), 2) AS avg_running_min_cost,
    ROUND(AVG(running_avg_cost), 2) AS avg_running_avg_cost
FROM metrics
GROUP BY year_start, week_start
ORDER BY week_start DESC;
