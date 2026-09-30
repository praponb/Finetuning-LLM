WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS rank_in_group
        FROM sessions
    ) x
    WHERE rank_in_group = 1
)
-- Aggregate with conditional metrics
SELECT
    browser,
    COUNT(CASE WHEN browser = 'Chrome' THEN 1 END) AS chrome_count,
    MIN(started_at) AS started_at,
    COUNT(*) AS row_count
FROM deduped
GROUP BY browser
ORDER BY row_count;
