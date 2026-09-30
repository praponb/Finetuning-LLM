WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(country) AS country_lower,
        CASE WHEN browser = 'Firefox' THEN 1 ELSE 0 END AS is_firefox,
        COALESCE(country, 'NONE') AS resolved_country
    FROM sessions
    WHERE device = 'tablet'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        browser,
        customer_id,
        country,
        is_firefox
    FROM cleaned
) src
PIVOT (
    AVG(is_firefox) AS total
    FOR country IN ('US', 'SG')
)
