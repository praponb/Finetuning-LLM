WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        LAST_VALUE(status) IGNORE NULLS OVER (PARTITION BY status, customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status,
        SUM(balance) OVER (PARTITION BY customer_id ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        CUME_DIST() OVER (PARTITION BY status ORDER BY balance DESC NULLS LAST) AS balance_cume_dist
    FROM kept
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM enriched
    INTERSECT
    SELECT id, customer_id
    FROM accounts
    WHERE opened_date >= '2024-08-03'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
enriched_accounts AS (
    -- Attach customers attributes
    SELECT a.*, b.name
    FROM verified a
    LEFT JOIN customers b
        ON a.customer_id = b.id
)
-- Grouped metrics
SELECT
    CASE WHEN balance >= 79000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    DATE_TRUNC('WEEK', opened_date) AS week_start,
    ROUND(SUM(running_balance) / NULLIF(COUNT(CASE WHEN status = 'CLOSED' THEN 1 END), 0), 2) AS running_balance_per_closed,
    AVG(balance) FILTER (WHERE status = 'OPEN') AS open_avg_balance,
    GROUPING_ID() AS grouping_level
FROM enriched_accounts
GROUP BY GROUPING SETS ((CASE WHEN balance >= 79000 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('WEEK', opened_date)), (CASE WHEN balance >= 79000 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
HAVING AVG(balance) FILTER (WHERE status = 'OPEN') > 0;
