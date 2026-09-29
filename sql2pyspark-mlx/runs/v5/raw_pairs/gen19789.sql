WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, v
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        key,
        v,
        element
    FROM flattened
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS element
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        v,
        element,
        UPPER(key) AS key_upper,
        CONCAT_WS('-', element, key) AS element_key_key,
        COALESCE(key, 'N/A') AS key_clean
    FROM lines
    WHERE created_at >= TO_DATE('2025-03-02')
      AND element = 'VIP'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LENGTH(element) AS element_length
    FROM base
    WHERE element IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY element ORDER BY element_length NULLS FIRST) AS element_length_cume_dist,
        DENSE_RANK() OVER (ORDER BY element_length) AS element_length_dense_rank
    FROM filtered
    WHERE element_length BETWEEN 21 AND 47
),
filtered_carts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        v,
        element,
        key_upper,
        element_key_key,
        key_clean,
        element_length,
        element_length_cume_dist,
        SUBSTRING(element, 1, 2) AS element_prefix,
        CASE WHEN element_length_cume_dist >= 0.62 THEN 'High' WHEN element_length_cume_dist >= 0.22 THEN 'Medium' ELSE 'Low' END AS element_length_cume_dist_tier
    FROM with_windows
    WHERE element IS NOT NULL
),
with_windows_carts AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_hist AS element_length_cume_dist_cume_dist,
        DENSE_RANK() OVER w_running AS element_length_dense_rank,
        MAX(element_length_cume_dist) OVER (PARTITION BY element) AS element_max_element_length_cume_dist
    FROM filtered_carts
    WINDOW w_hist AS (PARTITION BY element ORDER BY element_length_cume_dist DESC NULLS LAST), w_running AS (ORDER BY element_length DESC NULLS FIRST)
)
SELECT
    id,
    element,
    key_upper,
    element_key_key,
    element_prefix,
    element_length_cume_dist_tier,
    element_length_cume_dist_cume_dist,
    element_length_dense_rank
FROM with_windows_carts
ORDER BY element_length_cume_dist_cume_dist NULLS FIRST;
