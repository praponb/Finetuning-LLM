WITH base AS (
    SELECT
        *,
        DATE_FORMAT(shipped_date, 'yyyy-MM-dd') AS shipped_date_label,
        TRIM(status) AS clean_status,
        COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE delivered_date > DATE '2023-05-28'
      AND carrier IS DISTINCT FROM 'DHL'
)
SELECT
    CASE WHEN cost >= 92 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    DATE_TRUNC('WEEK', shipped_date) AS week_start,
    ROUND(SUM(cost) / NULLIF(COUNT(CASE WHEN status = 'IN_TRANSIT' THEN 1 END), 0), 2) AS cost_per_in_transit,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles
FROM base
GROUP BY ROLLUP (CASE WHEN cost >= 92 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('WEEK', shipped_date))
ORDER BY week_start DESC
