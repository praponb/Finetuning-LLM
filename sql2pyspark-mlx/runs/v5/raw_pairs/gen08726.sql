WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, element
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) li AS element
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(element) AS clean_element,
        TRIM(element) AS clean_element_2
    FROM flattened
    WHERE element IS NOT NULL
      AND element IS NOT DISTINCT FROM 'VIP'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN element = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle,
        COALESCE(element, 'NONE') AS element_filled
    FROM base
    WHERE element IS NOT NULL
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(is_bundle * 0.9, 2) AS adjusted_is_bundle,
        COALESCE(element, 'OTHER') AS element_filled_2
    FROM cleaned
    WHERE element IS NOT NULL
      AND element NOT IN ('VIP', 'WELCOME5', 'SPRING10')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        element,
        clean_element,
        is_bundle,
        adjusted_is_bundle,
        element_filled_2,
        SUBSTRING(element, 1, 3) AS element_prefix
    FROM cleaned_carts
    WHERE element IS NOT NULL
      AND is_bundle <= 54
),
cleaned_carts_2 AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(element) AS element_lower
    FROM filtered
    WHERE element IN ('BUNDLE', 'SPRING10')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        element,
        element_prefix,
        COUNT(is_bundle) OVER (PARTITION BY element) AS element_count_is_bundle,
        SUM(is_bundle) OVER (PARTITION BY element) AS element_sum_is_bundle
    FROM cleaned_carts_2
),
summary AS (
    -- Multi-level aggregation
    SELECT
        element,
        PERCENTILE_APPROX(element_count_is_bundle, ARRAY(0.25, 0.5, 0.75)) AS element_count_is_bundle_quartiles,
        COUNT_IF(element = 'BUNDLE') AS bundle_events,
        SORT_ARRAY(COLLECT_SET(element)) AS element_list,
        ROUND(AVG(element_count_is_bundle), 2) AS avg_element_count_is_bundle,
        MAX(element_count_is_bundle) FILTER (WHERE element = 'SPRING10') AS spring10_max_element_count_is_bundle
    FROM enriched
    GROUP BY element
)
SELECT
    element,
    element_count_is_bundle_quartiles,
    element_list,
    spring10_max_element_count_is_bundle
FROM summary
ORDER BY spring10_max_element_count_is_bundle NULLS LAST
