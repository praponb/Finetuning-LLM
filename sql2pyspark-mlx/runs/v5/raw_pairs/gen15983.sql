WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(cost) OVER (PARTITION BY status) status_max_cost,
        AVG(cost) OVER (PARTITION BY status, carrier ORDER BY cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) cost_avg_3,
        AVG(cost) OVER (ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) cost_avg_6_prior,
        COUNT(cost) OVER (PARTITION BY order_id) order_id_count_cost,
        AVG(cost) OVER (PARTITION BY order_id, status ORDER BY shipped_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) cost_avg_5
    FROM shipments
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    CASE WHEN cost_avg_3 >= 106 THEN 'HIGH' ELSE 'LOW' END AS cost_avg_3_tier,
    order_id,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END), 0), 2) AS cost_per_in_transit,
    SUM(CASE WHEN carrier = 'Kerry' THEN order_id_count_cost ELSE 0 END) AS kerry_order_id_count_cost,
    MAX(order_id_count_cost) AS max_order_id_count_cost,
    AVG(cost_avg_6_prior) FILTER (WHERE carrier = 'DHL') AS dhl_avg_cost_avg_6_prior
FROM metrics
GROUP BY activity_month, cost_avg_3_tier, order_id
ORDER BY cost_per_in_transit NULLS FIRST;
