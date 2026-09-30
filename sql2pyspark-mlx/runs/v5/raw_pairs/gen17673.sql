WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(cost) OVER (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_cost,
        COUNT(cost) OVER (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_cost,
        SUM(cost) OVER (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_cost,
        MAX(cost) OVER (PARTITION BY status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_cost,
        RANK() OVER (ORDER BY cost NULLS LAST) AS cost_rank
    FROM shipments
)
SELECT
    id,
    order_id,
    delivered_date,
    cost,
    status,
    running_avg_cost,
    running_count_cost,
    running_sum_cost,
    cost_rank
FROM metrics
