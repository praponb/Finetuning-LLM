WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(balance AS INT) AS balance_int,
        ROUND(balance * 1.07, 2) AS adjusted_balance,
        COALESCE(status, 'N/A') AS resolved_status
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND status = 'CLOSED'
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM filtered a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.opened_date BETWEEN sub.start_date AND sub.end_date
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY status ORDER BY monthly_fee DESC, id, subscription_id) AS monthly_fee_bucket,
        CUME_DIST() OVER (PARTITION BY account_type ORDER BY adjusted_balance DESC NULLS FIRST) AS adjusted_balance_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY plan, status ORDER BY adjusted_balance, id, subscription_id) AS seq_no
    FROM attributed
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        account_type,
        resolved_status,
        subscription_id,
        adjusted_balance_cume_dist
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY balance_int DESC NULLS LAST, id, subscription_id) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.merchant IS NULL
        )
),
filtered_accounts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        resolved_status,
        subscription_id,
        adjusted_balance_cume_dist,
        ROUND(adjusted_balance_cume_dist / subscription_id, 2) AS adjusted_balance_cume_dist_per_subscription_id
    FROM not_flagged
    WHERE adjusted_balance_cume_dist > 0.69
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.is_flagged
        )
)
SELECT
    id,
    account_type,
    subscription_id,
    adjusted_balance_cume_dist,
    adjusted_balance_cume_dist_per_subscription_id
FROM eligible
ORDER BY adjusted_balance_cume_dist
