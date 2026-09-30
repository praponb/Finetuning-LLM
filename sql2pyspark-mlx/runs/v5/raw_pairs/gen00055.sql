WITH cleaned AS (
    SELECT *, CAST(is_flagged AS INT) is_flagged_flag
    FROM transactions
    WHERE merchant IS NOT NULL
),
enriched AS (
    SELECT
        *,
        NTILE(4) OVER w_recent is_flagged_flag_bucket,
        MIN(amount) OVER (PARTITION BY merchant) merchant_min_amount
    FROM cleaned
    WINDOW w_recent AS (PARTITION BY merchant ORDER BY is_flagged_flag DESC, id ASC)
),
enriched_transactions AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        is_flagged,
        is_flagged_flag_bucket,
        merchant_min_amount,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY account_id ORDER BY is_flagged_flag_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_amount,
        FIRST_VALUE(merchant, TRUE) OVER (PARTITION BY merchant ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_merchant
    FROM enriched
),
scored AS (
    SELECT
        id,
        account_id,
        txn_type,
        is_flagged,
        is_flagged_flag_bucket,
        first_known_amount,
        COUNT(is_flagged_flag_bucket) OVER (PARTITION BY account_id, txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_count_is_flagged_flag_bucket,
        MIN(is_flagged_flag_bucket) OVER (PARTITION BY account_id, txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_is_flagged_flag_bucket,
        AVG(is_flagged_flag_bucket) OVER (PARTITION BY account_id, txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_is_flagged_flag_bucket,
        MAX(is_flagged_flag_bucket) OVER (PARTITION BY account_id, txn_type ORDER BY txn_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_is_flagged_flag_bucket,
        MAX(merchant_min_amount) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) merchant_min_amount_max_30d
    FROM enriched_transactions
    WHERE account_id IN (15, 19)
),
grouped AS (
    SELECT
        txn_type,
        ROUND(SUM(running_max_is_flagged_flag_bucket) / NULLIF(COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END), 0), 2) AS running_max_is_flagged_flag_bucket_per_debit,
        ROUND(SUM(running_max_is_flagged_flag_bucket) / NULLIF(COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END), 0), 2) AS running_max_is_flagged_flag_bucket_per_debit_2,
        PERCENTILE_APPROX(first_known_amount, 0.9) AS first_known_amount_p90,
        PERCENTILE_APPROX(running_max_is_flagged_flag_bucket, ARRAY(0.25, 0.5, 0.75)) AS running_max_is_flagged_flag_bucket_quartiles,
        COUNT(*) AS event_count,
        ROUND(SUM(running_min_is_flagged_flag_bucket) / NULLIF(COUNT(CASE WHEN txn_type = 'REFUND' THEN 1 END), 0), 2) AS running_min_is_flagged_flag_bucket_per_refund
    FROM scored
    GROUP BY txn_type
    HAVING ROUND(SUM(running_max_is_flagged_flag_bucket) / NULLIF(COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END), 0), 2) > 0
)
SELECT
    txn_type,
    running_max_is_flagged_flag_bucket_quartiles,
    running_min_is_flagged_flag_bucket_per_refund
FROM grouped
ORDER BY running_min_is_flagged_flag_bucket_per_refund DESC
