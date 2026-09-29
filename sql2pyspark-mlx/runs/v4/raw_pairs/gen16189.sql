WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(status, 'OTHER') AS status_clean
    FROM accounts
    WHERE opened_date > DATE '2023-09-03'
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.txn_time,
        x.transaction_amount
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.txn_time, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_keys AS (
    -- Set difference on keys
    SELECT account_type, id
    FROM with_last
    EXCEPT
    SELECT account_type, id
    FROM accounts
    WHERE balance BETWEEN 28000 AND 51000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN kept_keys k ON s.account_type = k.account_type AND s.id = k.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(balance) OVER w_recent AS account_type_sum_balance,
        AVG(transaction_amount) OVER w_recent AS account_type_avg_transaction_amount,
        DENSE_RANK() OVER (ORDER BY transaction_amount NULLS FIRST) AS transaction_amount_dense_rank,
        RANK() OVER (ORDER BY balance DESC NULLS LAST) AS balance_rank,
        MAX(balance) OVER (PARTITION BY customer_id, account_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS balance_max_90d
    FROM verified
    WINDOW w_recent AS (PARTITION BY account_type)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(account_type_avg_transaction_amount, 2) AS account_type_avg_transaction_amount_rounded,
        COALESCE(balance, 0) AS balance_filled,
        COALESCE(status, 'OTHER') AS status_filled
    FROM with_windows
    WHERE txn_time < '2023-03-05'
)
SELECT
    s.id,
    s.customer_id,
    s.account_type,
    s.balance,
    s.opened_date,
    s.status,
    s.transaction_amount,
    s.balance_rank,
    s.balance_max_90d,
    CASE WHEN account_type RLIKE '^(SAVINGS|LOAN)$' THEN 'GROUP_A' WHEN account_type RLIKE '^(CHECKING|CREDIT)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS account_type_zone,
    (SELECT MAX(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS max_account_balance
FROM cleaned s
