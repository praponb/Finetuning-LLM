WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(balance, 1) OVER (PARTITION BY customer_id ORDER BY opened_date, id) AS next_balance,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY balance, id) AS row_seq,
        LAG(balance, 2, 0) OVER (PARTITION BY account_type ORDER BY opened_date, id) AS prev_balance,
        NTILE(10) OVER (PARTITION BY status ORDER BY balance, id) AS balance_bucket
    FROM accounts
    WHERE customer_id IS NOT NULL
),
with_latest AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.account_type,
        s.next_balance,
        s.seq_no,
        s.row_seq,
        s.balance_bucket,
        latest.amount,
        latest.transaction_merchant
    FROM scored s
    JOIN LATERAL (
            SELECT c.amount, c.merchant AS transaction_merchant
            FROM transactions c
            WHERE c.account_id = s.id
              AND c.txn_time <= s.opened_date
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        next_balance,
        seq_no,
        row_seq,
        balance_bucket,
        LAG(seq_no, 1) OVER (ORDER BY amount, id) AS prev_seq_no,
        AVG(row_seq) OVER (PARTITION BY account_type ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS row_seq_avg_6_prior
    FROM with_latest
),
with_last AS (
    -- Latest matching transaction per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.next_balance,
        s.balance_bucket,
        s.row_seq_avg_6_prior,
        latest.transaction_amount,
        latest.txn_type
    FROM enriched s
    JOIN LATERAL (
            SELECT c.amount AS transaction_amount, c.txn_type
            FROM transactions c
            WHERE c.account_id = s.id
            ORDER BY c.txn_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    balance_bucket,
    row_seq_avg_6_prior,
    transaction_amount,
    txn_type
FROM with_last
ORDER BY txn_type DESC NULLS LAST;
