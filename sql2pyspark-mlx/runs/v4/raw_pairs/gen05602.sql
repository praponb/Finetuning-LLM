WITH base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(cost * 1.07, 2) AS cost_adjusted
    FROM shipments
    WHERE order_id IS NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('week', delivered_date) AS delivered_date_period
    FROM base
    WHERE delivered_date >= DATE '2023-06-11'
      AND order_id IS NOT NULL
      AND carrier IN ('DHL', 'Flash')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_adjusted,
        delivered_date_period,
        AVG(cost) OVER (ORDER BY shipped_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS cost_avg_2_prior,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date, id) AS seq_no
    FROM valid
)
-- Grouped metrics
SELECT
    CASE WHEN cost_adjusted >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END AS cost_adjusted_tier,
    seq_no,
    CASE WHEN cost_avg_2_prior >= 79 THEN 'LARGE' ELSE 'SMALL' END AS cost_avg_2_prior_tier,
    ROUND(AVG(cost_adjusted), 2) AS avg_cost_adjusted,
    COUNT_IF(status = 'LOST') AS lost_events
FROM with_windows
GROUP BY CASE WHEN cost_adjusted >= 78 THEN 'PREMIUM' ELSE 'STANDARD' END, seq_no, CASE WHEN cost_avg_2_prior >= 79 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY cost_avg_2_prior_tier;
