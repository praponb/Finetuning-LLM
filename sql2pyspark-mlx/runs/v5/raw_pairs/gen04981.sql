WITH lines AS (
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS element
    FROM carts
),
filtered AS (
    SELECT
        *,
        INITCAP(TRIM(element)) AS element_title,
        DATE_FORMAT(created_at, 'yyyy-MM-dd') AS created_at_label,
        COALESCE(element, 'N/A') AS resolved_element
    FROM lines
    WHERE element IS NOT NULL
      AND element IN ('VIP', 'BUNDLE', 'SPRING10')
),
cleaned AS (
    SELECT
        *,
        CAST(created_at AS DATE) AS created_at_day,
        LOWER(element) AS element_lower,
        COALESCE(element, 'N/A') AS resolved_element_2
    FROM filtered
    WHERE element IS NOT NULL
      AND created_at >= '2023-06-12'
),
base AS (
    SELECT
        id,
        created_at,
        element,
        element_title,
        resolved_element,
        created_at_day,
        element_lower,
        resolved_element_2,
        DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM cleaned
    WHERE element = 'WELCOME5'
)
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    ROUND(SUM(days_since_created_at) / NULLIF(COUNT(CASE WHEN element = 'WELCOME5' THEN 1 END), 0), 2) AS days_since_created_at_per_welcome5,
    SIZE(COLLECT_SET(element)) AS num_elements,
    MIN(created_at) AS first_seen
FROM base
GROUP BY activity_day
ORDER BY days_since_created_at_per_welcome5
