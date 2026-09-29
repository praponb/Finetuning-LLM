WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_type,
        balance,
        opened_date,
        status,
        DATE_ADD(opened_date, 72) AS opened_date_plus_72d,
        COALESCE(status, 'DIRECT') AS resolved_status
    FROM accounts
    WHERE opened_date > DATE '2023-05-28'
      AND status IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.merchant IN ('Grab', 'Lazada')
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, account_type ORDER BY balance DESC, id) AS row_num
    FROM not_flagged
    QUALIFY row_num = 1
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.opened_date,
        s.opened_date_plus_72d,
        s.resolved_status,
        s.row_num,
        top1.transaction_txn_type,
        top1.transaction_amount
    FROM ranked s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type, c.amount AS transaction_amount
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest_accounts AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.transaction_amount_2
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount_2
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        opened_date,
        opened_date_plus_72d,
        transaction_amount,
        LAST_VALUE(transaction_txn_type, TRUE) OVER (PARTITION BY transaction_txn_type ORDER BY transaction_amount_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_transaction_txn_type,
        SUM(transaction_amount) OVER (PARTITION BY transaction_txn_type ORDER BY opened_date_plus_72d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_transaction_amount
    FROM with_latest_accounts
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.transaction_amount_2,
        x.merchant
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.amount AS transaction_amount_2, c.merchant
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    opened_date_plus_72d,
    transaction_amount,
    last_known_transaction_txn_type,
    running_transaction_amount,
    transaction_amount_2
FROM with_last;
