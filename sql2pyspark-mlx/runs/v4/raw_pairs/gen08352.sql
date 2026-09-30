WITH with_dims AS (
    -- Attach customers attributes
    SELECT e.*, d.country
    FROM accounts e
    LEFT JOIN customers d
        ON e.customer_id = d.id
)
-- Aggregate with conditional metrics
SELECT
    country,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN status = 'CLOSED' THEN 1 END), 0), 2) AS balance_per_closed,
    COUNT(*) AS cnt,
    COUNT_IF(country = 'US') AS us_events
FROM with_dims
GROUP BY country
