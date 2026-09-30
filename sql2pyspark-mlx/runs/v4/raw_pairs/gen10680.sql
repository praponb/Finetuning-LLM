WITH base AS (
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        ROUND(cost, 1) AS cost_rounded
    FROM shipments
    WHERE shipped_date >= DATE '2023-11-11'
      AND order_id IS NOT NULL
      AND order_id = 35
)
SELECT
    *,
    PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY cost NULLS FIRST) AS cost_percent_rank,
    SUM(cost_rounded) OVER (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_rounded,
    RANK() OVER (PARTITION BY carrier ORDER BY cost_rounded DESC) AS cost_rounded_rank
FROM base;
