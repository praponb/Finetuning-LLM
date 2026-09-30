WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY carrier ORDER BY shipped_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cost,
        CUME_DIST() OVER (PARTITION BY carrier ORDER BY cost ASC NULLS FIRST) AS cost_cume_dist,
        FIRST_VALUE(carrier) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY delivered_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_carrier
    FROM shipments
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(carrier) AS carrier_upper,
        COALESCE(carrier, 'N/A') AS carrier_clean
    FROM enriched
    WHERE cost < 40
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        running_cost,
        cost_cume_dist,
        first_known_carrier,
        carrier_upper,
        carrier_clean,
        LOWER(carrier) AS carrier_lower
    FROM valid
    WHERE cost BETWEEN 73 AND 76
)
-- Grouped metrics
SELECT
    CASE WHEN cost >= 93 THEN 'LARGE' ELSE 'SMALL' END AS cost_tier,
    CASE WHEN running_cost >= 427 THEN 'HIGH' ELSE 'LOW' END AS running_cost_tier,
    carrier,
    APPROX_PERCENTILE(cost_cume_dist, 0.9) AS cost_cume_dist_p90,
    APPROX_PERCENTILE(cost_cume_dist, 0.9) AS cost_cume_dist_p90_2,
    APPROX_PERCENTILE(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles,
    COUNT(*) AS row_count,
    GROUPING(CASE WHEN cost >= 93 THEN 'LARGE' ELSE 'SMALL' END) AS gid
FROM base
GROUP BY GROUPING SETS ((CASE WHEN cost >= 93 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN running_cost >= 427 THEN 'HIGH' ELSE 'LOW' END, carrier), (CASE WHEN cost >= 93 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN running_cost >= 427 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN cost >= 93 THEN 'LARGE' ELSE 'SMALL' END), ())
HAVING cost_cume_dist_p90_2 < 0
ORDER BY running_cost_tier DESC NULLS FIRST;
