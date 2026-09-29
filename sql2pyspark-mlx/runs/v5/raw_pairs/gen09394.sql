WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        status,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY opened_date ASC, id ASC) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY account_type ORDER BY balance ASC NULLS LAST) AS balance_dense_rank,
        LAST_VALUE(balance) IGNORE NULLS OVER (PARTITION BY customer_id, account_type ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_balance
    FROM accounts
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        account_type,
        seq_no,
        balance_dense_rank,
        last_known_balance,
        AVG(last_known_balance) OVER (PARTITION BY account_type, status ORDER BY last_known_balance ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last_known_balance_avg_3,
        SUM(balance) OVER (ORDER BY last_known_balance ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance,
        AVG(balance) OVER (ORDER BY balance ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS balance_avg_3
    FROM enriched
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.seq_no,
        s.last_known_balance_avg_3,
        s.balance_avg_3,
        x.transaction_txn_type
    FROM metrics s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        balance_avg_3 / NULLIF(last_known_balance_avg_3, 0) AS balance_avg_3_per_last_known_balance_avg_3,
        CONCAT_WS('-', account_type, transaction_txn_type) AS account_type_transaction_txn_type_key,
        COALESCE(transaction_txn_type, 'OTHER') AS transaction_txn_type_clean
    FROM with_latest
    WHERE transaction_txn_type IS NOT DISTINCT FROM 'CREDIT'
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.account_id = 38
        )
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            transaction_txn_type,
            account_type,
            balance_avg_3
        FROM kept
    ) src
    PIVOT (
        SUM(balance_avg_3) AS amount,
        COUNT(1) AS cnt
        FOR account_type IN ('CHECKING', 'CREDIT')
    )
)
SELECT
    transaction_txn_type,
    CHECKING_amount,
    CHECKING_cnt,
    CREDIT_amount,
    CASE WHEN transaction_txn_type RLIKE '^(FEE|DEBIT)$' THEN 'GROUP_A' ELSE 'CORE' END AS transaction_txn_type_zone
FROM pivoted
ORDER BY transaction_txn_type DESC
LIMIT 20;
