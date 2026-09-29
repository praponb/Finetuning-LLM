WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(balance AS INT) balance_int,
        COALESCE(status, 'N/A') status_filled
    FROM accounts
    WHERE opened_date >= DATE '2023-02-28'
      AND account_type IN ('LOAN', 'SAVINGS', 'CHECKING')
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(balance, 2) OVER (PARTITION BY customer_id, account_type ORDER BY balance, id) prev_balance,
        SUM(balance) OVER (PARTITION BY account_type ORDER BY balance, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_balance
    FROM not_flagged
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM with_windows a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.opened_date >= sub.start_date
       AND a.opened_date < sub.end_date + INTERVAL 7 DAYS
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM matched
    INTERSECT ALL
    SELECT id
    FROM accounts
    WHERE balance BETWEEN 42000 AND 76000
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM matched s
    INNER JOIN kept_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN subscription_id >= 11 THEN 'LARGE' ELSE 'SMALL' END AS subscription_id_tier,
    subscription_id,
    COUNT(CASE WHEN account_type = 'CHECKING' THEN 1 END) AS checking_count,
    COUNT_IF(status = 'FROZEN') AS frozen_events,
    APPROX_PERCENTILE(prev_balance, 0.9) AS prev_balance_p90,
    COUNT(DISTINCT plan) AS distinct_plans,
    COUNT(*) FILTER (WHERE plan = 'premium') AS premium_rows
FROM reconciled
GROUP BY CASE WHEN subscription_id >= 11 THEN 'LARGE' ELSE 'SMALL' END, subscription_id
HAVING distinct_plans >= 1
