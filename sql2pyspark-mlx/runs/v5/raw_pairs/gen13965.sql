WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(value, 'unassigned') AS value_filled,
        COALESCE(value, 'NONE') AS value_filled_2
    FROM lines
    WHERE value IN ('WELCOME5', 'VIP')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(value) AS value_lower,
        COALESCE(value, 'N/A') AS value_clean
    FROM valid
    WHERE value IS NOT NULL
),
filtered_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(value) AS value_upper,
        COALESCE(value, 'OTHER') AS value_filled_3
    FROM filtered
    WHERE value = 'VIP'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        value,
        value_filled_2,
        value_lower,
        value_clean,
        value_upper,
        value_filled_3,
        INITCAP(TRIM(value)) AS value_title
    FROM filtered_carts
    WHERE value = 'BUNDLE'
      AND value IS NOT DISTINCT FROM 'WELCOME5'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        value,
        SIZE(COLLECT_SET(value)) AS num_values,
        COUNT(DISTINCT value) AS distinct_values,
        COUNT(*) AS event_count,
        COUNT_IF(value = 'SPRING10') AS spring10_events,
        COUNT(DISTINCT value) AS distinct_values_2,
        SORT_ARRAY(COLLECT_SET(value)) AS value_list
    FROM cleaned
    GROUP BY value
)
SELECT
    value,
    num_values,
    distinct_values,
    spring10_events,
    distinct_values_2,
    value_list,
    CASE WHEN value RLIKE '^(BUNDLE|WELCOME5)$' THEN 'OTHER_ZONE' WHEN value RLIKE '^(SPRING10|VIP)$' THEN 'CORE' ELSE 'GROUP_B' END AS value_zone
FROM summary
