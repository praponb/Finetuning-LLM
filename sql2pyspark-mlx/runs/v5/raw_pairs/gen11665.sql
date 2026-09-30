WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(balance) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_avg_3,
        ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY balance, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS seq_no_2
    FROM accounts
    WHERE status = 'OPEN'
),
latest_match AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.txn_time,
        latest.transaction_amount
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.txn_time, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time <= s.opened_date
        )
),
not_flagged_accounts AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.txn_time
        )
)
SELECT
    id,
    customer_id,
    account_type,
    balance,
    balance_avg_3,
    seq_no,
    CASE WHEN account_type RLIKE '^(LOAN|CHECKING)$' THEN 'GROUP_B' WHEN account_type RLIKE '^(SAVINGS|CREDIT)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS account_type_zone
FROM not_flagged_accounts
ORDER BY id DESC, customer_id
