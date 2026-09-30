WITH metrics AS (
    SELECT
        *,
        SUM(cost) OVER w_part AS running_cost,
        SUM(cost) OVER (PARTITION BY status ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_2,
        SUM(cost) OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost_3,
        COUNT(cost) OVER (PARTITION BY carrier) AS carrier_count_cost
    FROM shipments
    WHERE cost > 56
    WINDOW w_part AS (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    shipped_date,
    cost,
    running_cost,
    running_cost_2,
    running_cost_3,
    carrier_count_cost
FROM metrics
ORDER BY running_cost, cost DESC
