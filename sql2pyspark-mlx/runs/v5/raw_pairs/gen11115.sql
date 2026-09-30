WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        started_at,
        country,
        LENGTH(browser) AS browser_length,
        COALESCE(country, 'N/A') AS country_filled,
        COALESCE(country, 'NONE') AS country_clean
    FROM sessions
    WHERE started_at >= '2024-06-01'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        customer_id,
        country,
        browser_length
    FROM filtered
) src
PIVOT (
    SUM(browser_length) AS total,
    COUNT(1) AS cnt
    FOR country IN ('UK', 'DE')
);
