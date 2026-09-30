WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount * 1.07, 2) AS adjusted_amount,
        COALESCE(merchant, 'N/A') AS merchant_filled
    FROM transactions
    WHERE merchant IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    merchant,
    is_flagged,
    adjusted_amount,
    merchant_filled,
    ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time, id) AS row_seq,
    STDDEV_SAMP(amount) OVER (PARTITION BY merchant) AS merchant_stddev_samp_amount,
    COUNT(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_count_7d,
    MAX(adjusted_amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS adjusted_amount_max_90d
FROM cleaned;
