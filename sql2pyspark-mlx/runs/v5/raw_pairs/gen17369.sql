WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        CASE WHEN browser = 'Safari' THEN 1 ELSE 0 END AS is_safari,
        COALESCE(browser, 'NONE') AS browser_clean
    FROM sessions
    WHERE started_at >= DATE '2023-05-20'
      AND country IS NULL
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY country, customer_id ORDER BY is_safari, id) AS row_seq,
    LEAD(is_safari, 2) OVER (PARTITION BY customer_id ORDER BY is_safari, id) AS next_is_safari
FROM filtered
