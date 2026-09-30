WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        LENGTH(country) AS country_length,
        COALESCE(country, 'UNKNOWN') AS resolved_country
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-04-21 02:00:00'
)
-- Deduplicate with ROW_NUMBER
SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS row_num
FROM cleaned
QUALIFY row_num = 1
