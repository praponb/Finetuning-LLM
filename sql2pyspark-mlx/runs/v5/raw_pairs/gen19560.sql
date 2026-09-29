WITH exploded AS (
    SELECT
        id,
        created_at,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS value
),
filtered AS (
    SELECT
        *,
        HOUR(created_at) AS created_at_hour,
        LOWER(value) AS value_lower,
        COALESCE(value, 'OTHER') AS value_filled
    FROM exploded
    WHERE created_at >= TO_DATE('2025-03-02')
      AND TO_DATE(created_at) < '2023-06-22'
),
valid AS (
    SELECT
        *,
        CASE WHEN created_at_hour >= 90 THEN 'High' WHEN created_at_hour >= 19 THEN 'Medium' ELSE 'Low' END AS created_at_hour_band,
        COALESCE(created_at_hour, 0) AS created_at_hour_filled
    FROM filtered
    WHERE created_at > TIMESTAMP '2025-03-02 01:00:00'
      AND value IS NOT DISTINCT FROM 'BUNDLE'
),
base AS (
    SELECT
        id,
        value_filled,
        created_at_hour_band,
        created_at_hour_filled,
        TO_DATE(created_at) AS created_at_date,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM valid
    WHERE created_at > TIMESTAMP '2025-03-01 13:00:00'
      AND value IS NOT NULL
),
scored AS (
    SELECT *, CUME_DIST() OVER (ORDER BY created_at_hour_filled DESC) AS created_at_hour_filled_cume_dist
    FROM base
    WHERE YEAR(created_at_date) >= 2024
),
cleaned AS (
    SELECT *, CEIL(created_at_hour_filled_cume_dist) AS created_at_hour_filled_cume_dist_whole
    FROM scored
    WHERE YEAR(created_at_date) >= 2024
)
SELECT
    id,
    value_filled,
    created_at_hour_filled_cume_dist
FROM cleaned
ORDER BY created_at_hour_filled_cume_dist DESC, id ASC
