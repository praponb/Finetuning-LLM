WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.opened_date
        )
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC) AS row_num
    FROM not_flagged
    QUALIFY row_num = 1
),
not_flagged_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM ranked s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM not_flagged_accounts e
    LEFT JOIN subscriptions sub
        ON e.customer_id = sub.customer_id
       AND e.opened_date BETWEEN sub.start_date AND sub.end_date
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            account_type,
            customer_id,
            plan,
            balance
        FROM attributed
    ) src
    PIVOT (
        SUM(balance) AS total,
        COUNT(1) AS cnt
        FOR plan IN ('enterprise', 'standard')
    )
)
SELECT
    s.account_type,
    s.customer_id,
    s.enterprise_total,
    s.enterprise_cnt,
    s.standard_total,
    (SELECT MAX(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS max_account_balance
FROM wide s;
