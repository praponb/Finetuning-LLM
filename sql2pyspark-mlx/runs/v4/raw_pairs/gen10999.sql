WITH base AS (
    SELECT
        id,
        name,
        email,
        country,
        segment,
        signup_date,
        is_active,
        age,
        CAST(is_active AS INT) AS is_active_flag,
        DATEDIFF(CURRENT_DATE(), signup_date) AS days_since_signup_date,
        COALESCE(segment, 'NONE') AS segment_clean
    FROM customers
    WHERE signup_date > DATE '2024-01-18'
      AND city IS NOT NULL
)
SELECT
    CASE WHEN is_active_flag >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END AS is_active_flag_tier,
    COUNT_IF(country = 'US') AS us_events,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows,
    SORT_ARRAY(COLLECT_SET(segment)) AS segment_list
FROM base
GROUP BY is_active_flag_tier
HAVING us_events > 1
ORDER BY us_rows NULLS FIRST
