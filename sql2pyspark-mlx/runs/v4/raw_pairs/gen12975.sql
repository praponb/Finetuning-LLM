WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(cost, 0) cost_rounded
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-10-08')
      AND carrier IS NOT NULL
),
metrics AS (
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
        SUM(cost_rounded) OVER w_hist running_cost_rounded,
        LAG(cost, 2) OVER (ORDER BY cost, id) prev_cost,
        CUME_DIST() OVER (PARTITION BY status ORDER BY cost NULLS LAST) cost_cume_dist
    FROM filtered
    WINDOW w_hist AS (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ADD_MONTHS(shipped_date, 3) shipped_date_plus_months
    FROM metrics
    WHERE MONTH(shipped_date) IN (6, 10, 12)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END is_delivered,
        COALESCE(carrier, 'N/A') carrier_clean
    FROM cleaned
    WHERE order_id IS NOT NULL
),
metrics_shipments AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(cost, 2, 0) OVER (PARTITION BY carrier ORDER BY shipped_date, id) next_cost,
        AVG(running_cost_rounded) OVER w_recent running_cost_rounded_avg_2_prior
    FROM valid
    WINDOW w_recent AS (ORDER BY cost, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN prev_cost >= 87 THEN 'HIGH' ELSE 'LOW' END AS prev_cost_tier,
    DATE_TRUNC('QUARTER', shipped_date) AS quarter_start,
    SUM(next_cost) AS total_next_cost,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN status = 'LOST' THEN 1 END), 0), 2) AS cost_per_lost,
    COUNT_IF(status = 'IN_TRANSIT') AS in_transit_events
FROM metrics_shipments
GROUP BY CASE WHEN prev_cost >= 87 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', shipped_date)
ORDER BY quarter_start
