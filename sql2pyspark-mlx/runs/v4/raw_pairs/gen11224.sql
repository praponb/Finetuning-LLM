WITH filtered AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        UPPER(carrier) AS carrier_upper,
        COALESCE(carrier, 'OTHER') AS carrier_filled
    FROM shipments
    WHERE carrier IS NOT NULL
      AND delivered_date > '2025-08-21'
)
SELECT
    CASE WHEN cost >= 88 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    DATE_TRUNC('QUARTER', shipped_date) AS quarter_start,
    MAX(cost) FILTER (WHERE carrier = 'Kerry') AS kerry_max_cost,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    GROUPING_ID() AS grouping_id
FROM filtered
GROUP BY ROLLUP (CASE WHEN cost >= 88 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', shipped_date))
ORDER BY quarter_start DESC NULLS FIRST;
