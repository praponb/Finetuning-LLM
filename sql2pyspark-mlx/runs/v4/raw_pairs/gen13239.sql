WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM carts
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN value IN ('WELCOME5', 'BUNDLE') THEN 'Tier 1' ELSE 'Other' END AS value_group,
        COALESCE(value, 'DIRECT') AS value_filled
    FROM flattened
    WHERE created_at > DATE '2025-03-02'
      AND value IS NOT NULL
),
cleaned_carts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        value_group,
        value_filled,
        DAYOFWEEK(created_at) AS created_at_dayofweek,
        DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label,
        COALESCE(value, 'DIRECT') AS value_filled_2
    FROM cleaned
    WHERE value IN ('BUNDLE', 'WELCOME5')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CAST(created_at_dayofweek AS STRING) AS created_at_dayofweek_str
    FROM cleaned_carts
    WHERE created_at >= TO_TIMESTAMP('2025-03-02 14:00:00')
      AND HOUR(created_at) BETWEEN 10 AND 17
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        created_at,
        value_group,
        value_filled,
        created_at_dayofweek,
        created_at_label,
        value_filled_2,
        created_at_dayofweek_str,
        AVG(created_at_dayofweek) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 7200 PRECEDING AND CURRENT ROW) AS created_at_dayofweek_avg_2h,
        RANK() OVER w_hist AS created_at_dayofweek_rank
    FROM filtered
    WINDOW w_hist AS (ORDER BY created_at_dayofweek DESC NULLS FIRST)
),
filtered_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, HOUR(created_at) AS created_at_hour
    FROM with_windows
    WHERE created_at_dayofweek BETWEEN 22 AND 48
),
filtered_carts_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        created_at_dayofweek_rank / NULLIF(created_at_dayofweek_avg_2h, 0) AS created_at_dayofweek_rank_per_created_at_dayofweek_avg_2h,
        ROUND(created_at_dayofweek_avg_2h, 1) AS created_at_dayofweek_avg_2h_rounded
    FROM filtered_carts
    WHERE created_at > TIMESTAMP '2025-03-02 14:00:00'
)
SELECT
    id,
    created_at,
    value_group,
    value_filled,
    created_at_dayofweek_avg_2h,
    created_at_dayofweek_rank,
    created_at_hour,
    created_at_dayofweek_rank_per_created_at_dayofweek_avg_2h
FROM filtered_carts_2
