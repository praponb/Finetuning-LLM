WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(cost, 0) AS cost_rounded,
        ROUND(cost * 0.85, 2) AS cost_scaled
    FROM shipments
    WHERE shipped_date >= DATE '2023-10-05'
      AND carrier IS NOT NULL
      AND YEAR(delivered_date) = 2025
)
-- Running and moving aggregates
SELECT
    id,
    order_id,
    carrier,
    shipped_date,
    delivered_date,
    cost,
    status,
    cost_rounded,
    cost_scaled,
    FIRST_VALUE(carrier, TRUE) OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier,
    DENSE_RANK() OVER (PARTITION BY order_id ORDER BY cost_scaled DESC NULLS FIRST) AS cost_scaled_dense_rank
FROM filtered;
