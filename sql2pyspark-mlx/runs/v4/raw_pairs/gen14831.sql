WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount, id) seq_no,
        LEAD(amount, 2) OVER (PARTITION BY merchant, account_id ORDER BY amount, id) next_amount,
        LEAD(amount, 1, 0) OVER (ORDER BY amount, id) next_amount_2,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) amount_avg_6_prior
    FROM transactions
),
metrics_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(seq_no) OVER (PARTITION BY merchant) merchant_stddev_samp_seq_no,
        LAST_VALUE(amount_avg_6_prior, TRUE) OVER w_hist last_known_amount_avg_6_prior,
        LAG(next_amount, 1) OVER (PARTITION BY account_id ORDER BY next_amount_2, id) prev_next_amount
    FROM metrics
    WINDOW w_hist AS (PARTITION BY txn_type ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY merchant_stddev_samp_seq_no DESC NULLS FIRST, id) AS dedup_rank
    FROM metrics_transactions
    QUALIFY dedup_rank = 1
),
stats AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', txn_time) AS week_start,
        merchant_stddev_samp_seq_no,
        SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
        COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count,
        COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count_2,
        (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
        COUNT(*) AS cnt,
        MIN(txn_time) AS first_txn_time
    FROM deduped
    GROUP BY DATE_TRUNC('WEEK', txn_time), merchant_stddev_samp_seq_no
)
SELECT
    week_start,
    merchant_stddev_samp_seq_no,
    txn_type_list,
    debit_count,
    debit_count_2,
    span_minutes,
    cnt,
    first_txn_time
FROM stats
ORDER BY first_txn_time DESC, cnt DESC
