WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(carrier, ', ', status) AS carrier_status
    FROM shipments
    WHERE shipped_date >= TO_DATE('2023-07-25')
      AND carrier IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY carrier ORDER BY shipped_date) = 1
),
ranked_shipments AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY shipped_date DESC NULLS FIRST, id) = 1
),
filtered_shipments AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), shipped_date) AS days_since_shipped_date,
        COALESCE(cost, 0) AS cost_filled,
        COALESCE(carrier, 'OTHER') AS carrier_clean
    FROM ranked_shipments
    WHERE carrier IS DISTINCT FROM 'Kerry'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', shipped_date) AS year_start,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(days_since_shipped_date, ARRAY(0.25, 0.5, 0.75)) AS days_since_shipped_date_quartiles,
    ROUND(SUM(days_since_shipped_date) / NULLIF(COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END), 0), 2) AS days_since_shipped_date_per_in_transit
FROM filtered_shipments
GROUP BY DATE_TRUNC('YEAR', shipped_date)
ORDER BY row_count DESC
