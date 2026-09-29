WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(tag)) AS tag_title,
        HOUR(created_at) AS created_at_hour,
        COALESCE(tag, 'NONE') AS resolved_tag
    FROM lines
    WHERE tag IS NOT NULL
      AND tag <> 'WELCOME5'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        created_at,
        tag_title,
        created_at_hour,
        resolved_tag,
        LENGTH(tag) AS tag_length
    FROM filtered
    WHERE created_at > TIMESTAMP '2025-03-02 04:00:00'
      AND tag IS DISTINCT FROM 'BUNDLE'
),
filtered_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        GREATEST(created_at_hour, tag_length) AS greatest_created_at_hour_tag_length,
        created_at_hour - tag_length AS created_at_hour_minus_tag_length
    FROM cleaned
    WHERE tag_length >= 25
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(tag_length) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS tag_length_count_30m,
        CUME_DIST() OVER (ORDER BY created_at_hour_minus_tag_length DESC NULLS FIRST) AS created_at_hour_minus_tag_length_cume_dist,
        DENSE_RANK() OVER (ORDER BY created_at_hour_minus_tag_length NULLS FIRST) AS created_at_hour_minus_tag_length_dense_rank
    FROM filtered_carts
    WHERE created_at_hour > 68
),
filtered_carts_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(tag_length_count_30m, 2) AS tag_length_count_30m_rounded
    FROM scored
    WHERE created_at_hour_minus_tag_length > 35
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM filtered_carts_2
    INTERSECT
    SELECT id
    FROM carts
    WHERE customer_id = 12
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered_carts_2 s
    JOIN matched_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tag_length_count_30m >= 14 THEN 'PREMIUM' ELSE 'STANDARD' END AS tag_length_count_30m_tier,
    CASE WHEN created_at_hour_minus_tag_length_cume_dist >= 0.87 THEN 'HIGH' ELSE 'LOW' END AS created_at_hour_minus_tag_length_cume_dist_tier,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    ROUND(AVG(created_at_hour_minus_tag_length), 2) AS avg_created_at_hour_minus_tag_length,
    MAX(created_at) AS last_created_at
FROM verified
GROUP BY GROUPING SETS ((CASE WHEN tag_length_count_30m >= 14 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN created_at_hour_minus_tag_length_cume_dist >= 0.87 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('DAY', created_at)), (CASE WHEN tag_length_count_30m >= 14 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN created_at_hour_minus_tag_length_cume_dist >= 0.87 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING ROUND(AVG(created_at_hour_minus_tag_length), 2) >= 0;
