WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS key, attr_value
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        key,
        attr_value,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM exploded
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at,
        MONTH(created_at) AS created_at_month
    FROM lines
    WHERE created_at >= DATE '2025-03-02'
      AND value = 'WELCOME5'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM filtered
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 10 AND 17
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(days_since_created_at, 0) AS days_since_created_at_filled,
        days_since_created_at + created_at_month AS days_since_created_at_plus_created_at_month,
        COALESCE(key, 'NONE') AS resolved_key
    FROM verified
    WHERE created_at >= TIMESTAMP '2025-03-01 17:00:00'
      AND key IS NOT NULL
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN days_since_created_at >= 62 THEN 'HIGH' ELSE 'LOW' END AS days_since_created_at_tier,
        CASE WHEN days_since_created_at_plus_created_at_month >= 77 THEN 'LARGE' ELSE 'SMALL' END AS days_since_created_at_plus_created_at_month_tier,
        (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
        PERCENTILE_APPROX(created_at_month, 0.5) AS created_at_month_p50,
        GROUPING_ID() AS gid
    FROM base
    GROUP BY GROUPING SETS ((CASE WHEN days_since_created_at >= 62 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN days_since_created_at_plus_created_at_month >= 77 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN days_since_created_at >= 62 THEN 'HIGH' ELSE 'LOW' END))
)
SELECT
    days_since_created_at_tier,
    days_since_created_at_plus_created_at_month_tier,
    duration_min,
    created_at_month_p50,
    gid
FROM stats
ORDER BY days_since_created_at_plus_created_at_month_tier DESC
