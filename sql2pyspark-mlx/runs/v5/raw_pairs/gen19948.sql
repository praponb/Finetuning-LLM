WITH filtered AS (
    SELECT *, LENGTH(account_type) AS account_type_length
    FROM accounts
    WHERE opened_date >= '2023-11-01'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY account_type_length NULLS LAST, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
kept AS (
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_time < s.opened_date
        )
),
with_windows AS (
    SELECT
        *,
        SUM(account_type_length) OVER w_recent AS last4_sum_account_type_length,
        MIN(account_type_length) OVER w_recent AS last4_min_account_type_length,
        COUNT(account_type_length) OVER w_recent AS last4_count_account_type_length,
        ROW_NUMBER() OVER (PARTITION BY customer_id, account_type ORDER BY account_type_length DESC, id) AS event_seq
    FROM kept
    WHERE customer_id IS NULL
    WINDOW w_recent AS (PARTITION BY status, customer_id ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_latest AS (
    SELECT
        s.*,
        x.transaction_txn_time,
        x.txn_type
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.txn_time AS transaction_txn_time, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept_accounts AS (
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.is_flagged = FALSE
        )
),
kept_accounts_2 AS (
    SELECT s.*
    FROM kept_accounts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions x
            WHERE x.account_id = s.id
              AND x.txn_time <= s.transaction_txn_time
        )
)
SELECT
    customer_id,
    CASE WHEN balance >= 81000 THEN 'PREMIUM' ELSE 'STANDARD' END AS balance_tier,
    MIN(transaction_txn_time) AS first_seen,
    COUNT(*) FILTER (WHERE status = 'CLOSED') AS closed_rows,
    COUNT(DISTINCT txn_type) AS distinct_txn_types,
    DENSE_RANK() OVER (PARTITION BY customer_id ORDER BY COUNT(DISTINCT txn_type) DESC) AS distinct_txn_types_dense_rank
FROM kept_accounts_2
GROUP BY customer_id, balance_tier
HAVING COUNT(DISTINCT txn_type) > 1
ORDER BY customer_id DESC NULLS FIRST
