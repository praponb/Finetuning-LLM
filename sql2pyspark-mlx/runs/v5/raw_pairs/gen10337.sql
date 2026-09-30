WITH filtered AS (
    SELECT *, LENGTH(merchant) AS merchant_length
    FROM transactions
    WHERE merchant IS NOT DISTINCT FROM 'Lazada'
      AND txn_time >= DATE '2023-05-17'
),
with_dims AS (
    SELECT
        s.*,
        p.account_type,
        p.balance
    FROM filtered s
    LEFT JOIN accounts p
        ON s.account_id = p.id
    WHERE p.account_type <> 'CREDIT'
),
with_windows AS (
    SELECT
        *,
        LEAD(merchant_length) OVER w_running AS next_merchant_length,
        LEAD(balance) OVER w_running AS next_balance,
        ROW_NUMBER() OVER w_running AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY balance, id) AS row_seq
    FROM with_dims
    WINDOW w_running AS (PARTITION BY account_id, txn_type ORDER BY balance, id)
),
with_dims_transactions AS (
    SELECT
        src.*,
        b.opened_date,
        b.account_type AS account_account_type
    FROM with_windows src
    INNER JOIN accounts b
        ON src.account_id = b.id
),
scored AS (
    SELECT
        id,
        amount,
        txn_time,
        next_merchant_length,
        next_balance,
        seq_no,
        row_seq,
        opened_date,
        SUM(row_seq) OVER (PARTITION BY account_id ORDER BY merchant_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        AVG(row_seq) OVER (ORDER BY next_balance, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS row_seq_avg_5_prior
    FROM with_dims_transactions
),
scored_transactions AS (
    SELECT
        id,
        amount,
        next_merchant_length,
        row_seq,
        opened_date,
        running_row_seq,
        row_seq_avg_5_prior,
        CUME_DIST() OVER (ORDER BY next_merchant_length DESC NULLS FIRST) AS next_merchant_length_cume_dist,
        NTILE(3) OVER (ORDER BY row_seq_avg_5_prior DESC, id) AS row_seq_avg_5_prior_bucket,
        LAG(next_merchant_length, 2) OVER (ORDER BY row_seq_avg_5_prior, id) AS prev_next_merchant_length,
        DENSE_RANK() OVER (ORDER BY running_row_seq DESC) AS running_row_seq_dense_rank
    FROM scored
),
scored_transactions_2 AS (
    SELECT
        id,
        amount,
        next_merchant_length,
        row_seq,
        running_row_seq,
        next_merchant_length_cume_dist,
        NTILE(10) OVER (ORDER BY running_row_seq_dense_rank DESC, id) AS running_row_seq_dense_rank_bucket,
        NTILE(4) OVER (ORDER BY running_row_seq DESC, id) AS running_row_seq_bucket,
        NTILE(5) OVER (ORDER BY running_row_seq DESC, id) AS running_row_seq_bucket_2
    FROM scored_transactions
)
SELECT
    id,
    next_merchant_length,
    row_seq,
    running_row_seq,
    next_merchant_length_cume_dist,
    running_row_seq_dense_rank_bucket,
    running_row_seq_bucket
FROM scored_transactions_2
