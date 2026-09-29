WITH enriched AS (
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY account_id ORDER BY amount DESC, id ASC) amount_bucket,
        LAST_VALUE(merchant, TRUE) OVER (PARTITION BY merchant, account_id ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_merchant
    FROM transactions
),
with_windows AS (
    SELECT
        *,
        ROUND(amount_bucket / SUM(amount_bucket) OVER (PARTITION BY merchant, txn_type), 4) amount_bucket_share,
        FIRST_VALUE(merchant, TRUE) OVER (ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_merchant,
        SUM(amount) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_amount
    FROM enriched
),
metrics AS (
    SELECT
        *,
        SUM(running_amount) OVER w_grp cumulative_running_amount,
        LEAD(amount_bucket_share, 2, 0) OVER w_ordered next_amount_bucket_share,
        SUM(amount_bucket_share) OVER w_part cumulative_amount_bucket_share
    FROM with_windows
    WINDOW w_grp AS (ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY account_id, txn_type ORDER BY txn_time ASC, id ASC), w_part AS (PARTITION BY txn_type ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched_transactions AS (
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.amount,
        src.merchant,
        src.is_flagged,
        src.amount_bucket,
        src.amount_bucket_share,
        src.next_amount_bucket_share,
        b.balance,
        b.opened_date,
        b.account_type
    FROM metrics src
    INNER JOIN accounts b
        ON src.account_id = b.id
    WHERE b.account_type = 'CREDIT'
),
with_dims AS (
    SELECT
        e.id,
        e.account_id,
        e.txn_type,
        e.amount_bucket,
        b.status,
        b.opened_date AS account_opened_date
    FROM enriched_transactions e
    INNER JOIN accounts b
        ON e.account_id = b.id
),
enriched_transactions_2 AS (
    SELECT
        *,
        AVG(amount_bucket) OVER (PARTITION BY status) status_avg_amount_bucket,
        NTILE(5) OVER (PARTITION BY status ORDER BY amount_bucket ASC, id ASC) amount_bucket_bucket,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount_bucket ASC, id ASC) seq_no,
        LAG(amount_bucket, 1) OVER (PARTITION BY status ORDER BY account_opened_date ASC, id ASC) prev_amount_bucket
    FROM with_dims
)
SELECT
    dim.account_type,
    s.txn_type,
    COUNT(*) AS cnt,
    MIN(s.seq_no) AS min_seq_no,
    GROUPING_ID() AS gid
FROM enriched_transactions_2 s
INNER JOIN accounts dim ON s.account_id = dim.id
GROUP BY ROLLUP (dim.account_type, s.txn_type)
ORDER BY account_type ASC NULLS LAST;
