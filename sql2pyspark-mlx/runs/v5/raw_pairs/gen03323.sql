WITH base AS (
    SELECT *, CAST(balance AS INT) balance_int
    FROM accounts
    WHERE opened_date >= DATE '2023-07-20'
),
eligible AS (
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
),
with_windows AS (
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        balance_int,
        NTILE(10) OVER (PARTITION BY customer_id, account_type ORDER BY balance_int DESC, id) balance_int_bucket,
        AVG(balance) OVER (ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) balance_avg_5
    FROM eligible
),
ranked AS (
    SELECT
        id,
        balance,
        balance_int,
        balance_int_bucket,
        balance_avg_5
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY balance_avg_5 DESC, id) AS rn
        FROM with_windows
    ) t
    WHERE rn = 1
),
eligible_accounts AS (
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time < '2024-05-01'
        )
)
SELECT
    id,
    balance,
    balance_int_bucket,
    balance_avg_5
FROM eligible_accounts
ORDER BY balance_avg_5 DESC, id DESC;
