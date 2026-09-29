WITH matched_keys AS (
    -- Set difference on keys
    SELECT browser
    FROM sessions
    EXCEPT
    SELECT browser
    FROM sessions
    WHERE HOUR(started_at) BETWEEN 7 AND 15
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions s
    INNER JOIN matched_keys k ON s.browser = k.browser
),
matched_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM reconciled
    EXCEPT ALL
    SELECT id
    FROM sessions
    WHERE device = 'tablet'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN matched_keys_2 k ON s.id = k.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
),
not_flagged_sessions AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
        )
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    (SELECT AVG(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS avg_account_balance
FROM not_flagged_sessions s
