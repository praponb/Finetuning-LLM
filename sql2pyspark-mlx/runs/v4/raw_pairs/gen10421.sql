WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        attr_key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS attr_key, v
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(attr_key) AS attr_key_lower
    FROM lines
    WHERE attr_key IS NOT NULL
      AND attr_key = 'device'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(attr_key) AS attr_key_lower_2,
        CASE WHEN attr_key = 'utm_source' THEN 1 ELSE 0 END AS is_utm_source,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_clean
    FROM filtered
    WHERE attr_key IS NOT NULL
),
cleaned_carts AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        FLOOR(is_utm_source) AS is_utm_source_whole,
        COALESCE(attr_key, 'N/A') AS resolved_attr_key
    FROM cleaned
    WHERE is_utm_source BETWEEN 32 AND 79
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(is_utm_source_whole, 1) AS is_utm_source_whole_rounded,
        UPPER(attr_key) AS attr_key_upper
    FROM cleaned_carts
    WHERE is_utm_source_whole BETWEEN 37 AND 71
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY attr_key ORDER BY is_utm_source NULLS LAST) AS is_utm_source_dense_rank,
        RANK() OVER (PARTITION BY attr_key ORDER BY is_utm_source NULLS FIRST) AS is_utm_source_rank,
        STDDEV_SAMP(is_utm_source) OVER (PARTITION BY attr_key) AS attr_key_stddev_samp_is_utm_source
    FROM valid
),
valid_carts AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        attr_key,
        is_utm_source,
        attr_key_clean,
        is_utm_source_whole,
        resolved_attr_key,
        is_utm_source_rank,
        attr_key_stddev_samp_is_utm_source,
        ROUND(is_utm_source_whole_rounded, 2) AS is_utm_source_whole_rounded_rounded,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_clean_2
    FROM scored
    WHERE attr_key IS NOT NULL
)
-- Multi-level aggregation
SELECT
    attr_key,
    ROUND(AVG(attr_key_stddev_samp_is_utm_source), 2) AS avg_attr_key_stddev_samp_is_utm_source,
    SUM(CASE WHEN attr_key = 'gift' THEN is_utm_source_whole_rounded_rounded ELSE 0 END) AS gift_is_utm_source_whole_rounded_rounded
FROM valid_carts
GROUP BY 1
HAVING ROUND(AVG(attr_key_stddev_samp_is_utm_source), 2) < 0;
