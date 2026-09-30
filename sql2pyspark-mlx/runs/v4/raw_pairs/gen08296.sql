WITH base AS (
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        CASE WHEN country = 'DE' THEN 1 ELSE 0 END is_de,
        TO_DATE(started_at) started_at_date
    FROM sessions
    WHERE browser IS NOT NULL
),
enriched AS (
    SELECT a.*, p.email
    FROM base a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
base_sessions AS (
    SELECT
        *,
        ADD_MONTHS(started_at_date, 6) started_at_date_plus_months,
        CONCAT(browser, ' - ', country) browser_country,
        COALESCE(browser, 'NONE') resolved_browser
    FROM enriched
    WHERE email LIKE '%@gmail.com'
)
SELECT *
FROM (
    SELECT
        country,
        browser,
        is_de
    FROM base_sessions
) src
PIVOT (
    AVG(is_de) AS total,
    COUNT(1) AS cnt
    FOR browser IN ('Safari' AS safari, 'Edge' AS edge, 'Chrome' AS chrome)
)
