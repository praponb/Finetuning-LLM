WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(country, 'Unknown') country_filled
    FROM sessions
    WHERE device IN ('tablet', 'mobile', 'desktop')
)
-- Deduplicate with ROW_NUMBER
SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id) AS latest_rank
FROM cleaned
QUALIFY latest_rank = 1
