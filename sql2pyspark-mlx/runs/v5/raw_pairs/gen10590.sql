WITH enriched AS (
    -- Ranking and gap features
    SELECT *, FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country
    FROM sessions
    WHERE browser = 'Chrome'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT country, customer_id
    FROM enriched
    INTERSECT ALL
    SELECT country, customer_id
    FROM sessions
    WHERE browser IN ('Chrome', 'Firefox')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.country = k.country AND s.customer_id = k.customer_id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_type <> 'page_view'
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT device, id
    FROM eligible
    EXCEPT ALL
    SELECT device, id
    FROM sessions
    WHERE country NOT IN ('UK', 'TH')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN kept_keys k ON s.device = k.device AND s.id = k.id
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        customer_id,
        COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count,
        COUNT_IF(device = 'desktop') AS desktop_events
    FROM reconciled
    GROUP BY customer_id
)
SELECT
    s.customer_id,
    s.desktop_count,
    (SELECT SUM(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS sum_account_balance
FROM totals s
ORDER BY customer_id DESC
