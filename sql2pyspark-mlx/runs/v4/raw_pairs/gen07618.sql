WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(merchant) AS clean_merchant
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant IN ('Grab', '7-Eleven')
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'UNKNOWN') AS merchant_filled
    FROM base
    WHERE is_flagged
),
remaining_keys AS (
    -- Set difference on keys
    SELECT txn_type, merchant
    FROM filtered
    EXCEPT ALL
    SELECT txn_type, merchant
    FROM transactions
    WHERE amount < 3400
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN remaining_keys k ON s.txn_type = k.txn_type AND s.merchant = k.merchant
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(is_flagged_flag) OVER (PARTITION BY merchant) AS merchant_stddev_samp_is_flagged_flag,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) AS amount_share,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id), 4) AS amount_share_2
    FROM reconciled
),
filtered_transactions AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        clean_merchant,
        is_flagged_flag,
        merchant_filled,
        merchant_stddev_samp_is_flagged_flag,
        amount_share,
        amount_share_2,
        CASE WHEN merchant IN ('Starbucks', 'Grab') THEN 'Tier 1' ELSE 'Other' END AS merchant_group,
        COALESCE(merchant, 'NONE') AS merchant_filled_2
    FROM enriched
    WHERE amount_share_2 < 0.65
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(amount_share_2) OVER (PARTITION BY account_id) AS account_id_stddev_samp_amount_share_2,
        CUME_DIST() OVER (ORDER BY merchant_stddev_samp_is_flagged_flag NULLS LAST) AS merchant_stddev_samp_is_flagged_flag_cume_dist,
        AVG(merchant_stddev_samp_is_flagged_flag) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS merchant_stddev_samp_is_flagged_flag_avg_30d
    FROM filtered_transactions
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    CASE WHEN account_id_stddev_samp_amount_share_2 >= 1 THEN 'HIGH' ELSE 'LOW' END AS account_id_stddev_samp_amount_share_2_tier,
    SUM(CASE WHEN txn_type = 'REFUND' THEN merchant_stddev_samp_is_flagged_flag ELSE 0 END) AS refund_merchant_stddev_samp_is_flagged_flag,
    MAX(txn_time) AS last_seen,
    SIZE(COLLECT_SET(merchant)) AS num_merchants
FROM scored
GROUP BY DATE_TRUNC('MONTH', txn_time), CASE WHEN account_id_stddev_samp_amount_share_2 >= 1 THEN 'HIGH' ELSE 'LOW' END
ORDER BY refund_merchant_stddev_samp_is_flagged_flag NULLS LAST
