WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS value
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN value = 'WELCOME5' THEN 1 ELSE 0 END AS is_welcome5,
        DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label,
        COALESCE(value, 'UNKNOWN') AS value_filled
    FROM exploded
    WHERE created_at > TIMESTAMP '2025-03-02 03:00:00'
      AND value IN ('VIP', 'SPRING10', 'WELCOME5')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY value ORDER BY is_welcome5) AS is_welcome5_cume_dist,
        CUME_DIST() OVER (PARTITION BY value ORDER BY is_welcome5 DESC NULLS LAST) AS is_welcome5_cume_dist_2
    FROM cleaned
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        value,
        is_welcome5,
        value_filled,
        is_welcome5_cume_dist,
        DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label_2,
        COALESCE(value, 'DIRECT') AS value_filled_2
    FROM metrics
    WHERE value IS NOT NULL
      AND TO_DATE(created_at) >= '2023-09-23'
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(value) AS value_lower
    FROM valid
    WHERE created_at > DATE '2025-03-01'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        value,
        is_welcome5,
        is_welcome5_cume_dist,
        created_at_label_2,
        value_filled_2,
        value_lower,
        LENGTH(value) AS value_length,
        COALESCE(value, 'UNKNOWN') AS value_clean
    FROM cleaned_carts
    WHERE created_at > DATE '2025-03-02'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        created_at,
        value,
        is_welcome5,
        is_welcome5_cume_dist,
        created_at_label_2,
        value_filled_2,
        value_lower,
        value_length,
        value_clean,
        MAX(value_length) OVER w_recent AS value_max_value_length,
        PERCENT_RANK() OVER (PARTITION BY value ORDER BY is_welcome5_cume_dist DESC) AS is_welcome5_cume_dist_percent_rank,
        MAX(is_welcome5) OVER w_recent AS value_max_is_welcome5
    FROM base
    WINDOW w_recent AS (PARTITION BY value)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    CASE WHEN value_max_value_length >= 88 THEN 'LARGE' ELSE 'SMALL' END AS value_max_value_length_tier,
    value,
    COUNT(CASE WHEN value = 'SPRING10' THEN 1 END) AS spring10_count,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('DAY', created_at) ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
FROM enriched
GROUP BY DATE_TRUNC('DAY', created_at), CASE WHEN value_max_value_length >= 88 THEN 'LARGE' ELSE 'SMALL' END, value
ORDER BY value DESC NULLS FIRST
