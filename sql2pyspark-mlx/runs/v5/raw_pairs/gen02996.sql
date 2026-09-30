WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.duration_sec BETWEEN 196 AND 258
        )
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    CASE WHEN browser RLIKE '^(Edge|Firefox)$' THEN 'PRIORITY' WHEN browser RLIKE '^(Chrome|Safari)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS browser_zone,
    (SELECT MAX(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS max_account_balance
FROM not_flagged s
ORDER BY browser NULLS FIRST;
