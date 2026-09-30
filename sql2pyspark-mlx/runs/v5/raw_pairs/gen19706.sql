WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        ABS(balance) AS abs_balance,
        COALESCE(status, 'DIRECT') AS status_filled
    FROM accounts
    WHERE opened_date > DATE '2023-04-15'
      AND status IS NOT NULL
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM cleaned src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.opened_date >= sub.start_date
       AND src.opened_date < sub.end_date + INTERVAL '30' DAY
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM matched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND HOUR(ch.txn_time) BETWEEN 7 AND 13
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time >= s.opened_date
        )
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_type = 'REFUND'
        )
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    abs_balance,
    status_filled,
    subscription_id,
    plan,
    LEAD(monthly_fee, 2) OVER (PARTITION BY status ORDER BY balance, id, subscription_id) AS next_monthly_fee,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY abs_balance DESC, id, subscription_id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY balance DESC, id, subscription_id) AS row_seq_2,
    NTILE(10) OVER (PARTITION BY status ORDER BY balance DESC, id, subscription_id) AS balance_bucket,
    NTILE(4) OVER (PARTITION BY status, account_type ORDER BY monthly_fee, id, subscription_id) AS monthly_fee_bucket
FROM eligible
