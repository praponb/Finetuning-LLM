WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(balance) AS balance_whole
    FROM eligible
    WHERE status IS NOT NULL
      AND opened_date >= TO_DATE('2023-05-28')
),
eligible_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND NOT ch.is_flagged
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < s.opened_date
        )
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_whole,
        balance * balance_whole AS balance_times_balance_whole,
        UPPER(status) AS status_upper
    FROM not_flagged
    WHERE status IS NOT NULL
      AND balance <= 76000
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        balance,
        opened_date,
        status,
        balance_whole,
        balance_times_balance_whole,
        status_upper,
        LAG(balance_times_balance_whole, 1) OVER (PARTITION BY customer_id ORDER BY balance, id) AS prev_balance_times_balance_whole,
        FIRST_VALUE(status, TRUE) OVER (ORDER BY balance_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_status,
        SUM(balance_times_balance_whole) OVER (PARTITION BY status ORDER BY balance_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance_times_balance_whole,
        SUM(balance) OVER (PARTITION BY status ORDER BY balance_times_balance_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        LAST_VALUE(balance, TRUE) OVER (PARTITION BY customer_id ORDER BY balance_times_balance_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
    FROM kept
    WHERE opened_date BETWEEN '2023-06-01' AND '2025-05-01'
)
-- Aggregate with conditional metrics
SELECT
    status,
    CASE WHEN balance_times_balance_whole >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_times_balance_whole_tier,
    COUNT(*) FILTER (WHERE status = 'CLOSED') AS closed_rows,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    ROUND(AVG(balance_whole), 2) AS avg_balance_whole
FROM metrics
GROUP BY ROLLUP (status, CASE WHEN balance_times_balance_whole >= 63 THEN 'PREMIUM' ELSE 'STANDARD' END);
