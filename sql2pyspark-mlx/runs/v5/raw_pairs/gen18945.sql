WITH with_latest AS (
    SELECT
        s.id,
        s.customer_id,
        s.opened_date,
        s.status,
        top1.amount
    FROM accounts s
    JOIN LATERAL (
            SELECT c.amount
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last AS (
    SELECT
        s.id,
        s.status,
        s.amount,
        x.txn_type
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last_accounts AS (
    SELECT
        s.id,
        s.amount,
        x.transaction_txn_type
    FROM with_last s
    JOIN LATERAL (
            SELECT c.txn_type AS transaction_txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY transaction_txn_type ORDER BY amount DESC NULLS FIRST) AS row_num
        FROM with_last_accounts
    ) x
    WHERE row_num = 1
),
eligible AS (
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM transactions ch
            WHERE ch.account_id = s.id
              AND ch.txn_type NOT IN ('CREDIT', 'FEE')
        )
),
with_windows AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY transaction_txn_type ORDER BY amount DESC, id ASC) AS amount_bucket,
        LAG(amount, 1) OVER (PARTITION BY transaction_txn_type ORDER BY row_num ASC, id ASC) AS prev_amount,
        AVG(amount) OVER (PARTITION BY transaction_txn_type ORDER BY row_num ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_5_prior
    FROM eligible
    WHERE row_num <= 1
),
metrics AS (
    SELECT
        id,
        transaction_txn_type,
        row_num,
        DENSE_RANK() OVER w_recent AS amount_dense_rank,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM with_windows
    WINDOW w_recent AS (ORDER BY amount DESC NULLS FIRST), w_ordered AS (PARTITION BY transaction_txn_type ORDER BY amount_bucket DESC, id ASC)
)
SELECT
    id,
    transaction_txn_type,
    amount_dense_rank,
    seq_no
FROM metrics
ORDER BY id ASC NULLS FIRST, seq_no ASC;
