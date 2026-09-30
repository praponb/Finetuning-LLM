WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        browser,
        country,
        DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at,
        COALESCE(country, 'NONE') AS country_filled
    FROM sessions
    WHERE started_at < '2025-08-01'
)
-- Deduplicate with ROW_NUMBER
SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY browser ORDER BY days_since_started_at DESC NULLS LAST, id) = 1
