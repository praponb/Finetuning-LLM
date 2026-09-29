WITH kept_keys AS (
    SELECT customer_id, country
    FROM sessions
    EXCEPT DISTINCT
    SELECT customer_id, country
    FROM sessions
    WHERE country IN ('JP', 'DE')
),
verified AS (
    SELECT s.*
    FROM sessions s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id AND s.country = k.country
)
SELECT
    browser,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    country,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    SIZE(COLLECT_SET(browser)) AS num_browsers
FROM verified
GROUP BY browser, quarter_start, country
HAVING distinct_customer_ids >= 1;
