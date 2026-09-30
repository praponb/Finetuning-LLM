WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        FIRST_VALUE(merchant, TRUE) OVER (ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_merchant,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_sum_90d,
        DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY amount DESC NULLS FIRST) AS amount_dense_rank,
        STDDEV_SAMP(amount) OVER (PARTITION BY txn_type) AS txn_type_stddev_samp_amount
    FROM transactions
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    CASE WHEN amount_dense_rank >= 6 THEN 'LARGE' ELSE 'SMALL' END AS amount_dense_rank_tier,
    AVG(amount_sum_90d) FILTER (WHERE merchant = 'Shell') AS shell_avg_amount_sum_90d,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS span_minutes,
    AVG(amount_sum_90d) FILTER (WHERE merchant = 'Lazada') AS lazada_avg_amount_sum_90d,
    COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count,
    SUM(CASE WHEN txn_type = 'DEBIT' THEN txn_type_stddev_samp_amount ELSE 0 END) AS debit_txn_type_stddev_samp_amount
FROM scored
GROUP BY 1, 2
HAVING COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) > 1
ORDER BY year_start DESC NULLS LAST
