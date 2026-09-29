SELECT
    *,
    SUM(cost) OVER (PARTITION BY carrier ORDER BY delivered_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
    CUME_DIST() OVER (ORDER BY cost ASC NULLS FIRST) AS cost_cume_dist,
    ROUND(cost / SUM(cost) OVER (PARTITION BY order_id), 4) AS cost_share,
    SUM(cost) OVER (ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_2
FROM shipments;
