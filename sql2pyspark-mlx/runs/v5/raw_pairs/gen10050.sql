WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM transactions
    WHERE txn_time > TIMESTAMP '2023-09-02 07:00:00'
      AND amount IS NOT NULL
),
enriched AS (
    -- Attach accounts attributes
    SELECT e.*, a.balance
    FROM filtered e
    INNER JOIN accounts a
        ON e.account_id = a.id
    WHERE a.account_type IN ('CREDIT', 'LOAN', 'CHECKING')
),
enriched_transactions AS (
    -- Ranking and gap features
    SELECT
        id,
        txn_time,
        is_flagged,
        is_flagged_flag,
        is_flagged_label,
        balance,
        NTILE(3) OVER (ORDER BY is_flagged_flag DESC, id) AS is_flagged_flag_bucket,
        SUM(is_flagged_flag) OVER (PARTITION BY txn_type, merchant ORDER BY is_flagged_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_flagged_flag,
        NTILE(3) OVER (PARTITION BY merchant ORDER BY amount, id) AS amount_bucket
    FROM enriched
),
enriched_transactions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_time,
        balance,
        LEAD(amount_bucket) OVER (ORDER BY amount_bucket, id) AS next_amount_bucket,
        LAG(amount_bucket) OVER (ORDER BY amount_bucket, id) AS prev_amount_bucket,
        LAG(is_flagged_flag_bucket) OVER (ORDER BY amount_bucket, id) AS prev_is_flagged_flag_bucket,
        LEAD(is_flagged_flag_bucket) OVER (ORDER BY amount_bucket, id) AS next_is_flagged_flag_bucket,
        ROW_NUMBER() OVER (ORDER BY amount_bucket, id) AS event_seq,
        AVG(is_flagged_flag_bucket) OVER (ORDER BY balance, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS is_flagged_flag_bucket_avg_3
    FROM enriched_transactions
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', txn_time) AS week_start,
    CASE WHEN next_amount_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS next_amount_bucket_tier,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(txn_time) AS first_txn_time,
    ROUND(AVG(prev_is_flagged_flag_bucket), 2) AS avg_prev_is_flagged_flag_bucket,
    PERCENTILE_APPROX(is_flagged_flag_bucket_avg_3, ARRAY(0.25, 0.5, 0.75)) AS is_flagged_flag_bucket_avg_3_quartiles
FROM enriched_transactions_2
GROUP BY 1, 2
HAVING avg_prev_is_flagged_flag_bucket > 0;
