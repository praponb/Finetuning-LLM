WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        HOUR(txn_time) AS txn_time_hour,
        LOWER(merchant) AS merchant_lower,
        COALESCE(merchant, 'OTHER') AS merchant_filled
    FROM transactions
    WHERE txn_time > DATE '2023-11-08'
      AND merchant IS NOT NULL
      AND is_flagged
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'DIRECT') AS merchant_filled_2
    FROM valid
    WHERE merchant IS NOT NULL
      AND amount BETWEEN 1900 AND 3700
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        txn_time_hour,
        merchant_lower,
        merchant_filled,
        is_flagged_flag,
        merchant_filled_2,
        FIRST_VALUE(is_flagged_flag) IGNORE NULLS OVER (ORDER BY is_flagged_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_is_flagged_flag,
        LAST_VALUE(is_flagged_flag) IGNORE NULLS OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_flagged_flag,
        LAST_VALUE(merchant) IGNORE NULLS OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_merchant,
        ROW_NUMBER() OVER (PARTITION BY txn_type, merchant ORDER BY is_flagged_flag DESC, id) AS row_seq,
        SUM(is_flagged_flag) OVER (PARTITION BY account_id, txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_flagged_flag
    FROM cleaned
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        txn_time_hour,
        merchant_lower,
        merchant_filled,
        is_flagged_flag,
        merchant_filled_2,
        first_known_is_flagged_flag,
        last_known_is_flagged_flag,
        last_known_merchant,
        row_seq,
        cumulative_is_flagged_flag,
        ROUND(row_seq / SUM(row_seq) OVER (PARTITION BY txn_type, account_id), 4) AS row_seq_share,
        NTILE(4) OVER (PARTITION BY account_id ORDER BY txn_time_hour DESC, id) AS txn_time_hour_bucket
    FROM scored
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY first_known_is_flagged_flag DESC, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        txn_time_hour,
        merchant_lower,
        merchant_filled,
        is_flagged_flag,
        merchant_filled_2,
        first_known_is_flagged_flag,
        last_known_is_flagged_flag,
        last_known_merchant,
        row_seq,
        cumulative_is_flagged_flag,
        row_seq_share,
        txn_time_hour_bucket,
        rank_in_group,
        LEAD(txn_time_hour, 1) OVER (PARTITION BY txn_type ORDER BY first_known_is_flagged_flag, id) AS next_txn_time_hour,
        NTILE(3) OVER (PARTITION BY txn_type, account_id ORDER BY first_known_is_flagged_flag, id) AS first_known_is_flagged_flag_bucket,
        LAST_VALUE(row_seq) IGNORE NULLS OVER (ORDER BY row_seq_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq
    FROM deduped
)
-- Aggregate with conditional metrics
SELECT
    merchant,
    CASE WHEN first_known_is_flagged_flag_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS first_known_is_flagged_flag_bucket_tier,
    CASE WHEN last_known_row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS last_known_row_seq_tier,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types,
    SIZE(COLLECT_SET(txn_type)) AS num_txn_types_2,
    COUNT_IF(merchant = 'Grab') AS grab_events
FROM metrics
GROUP BY GROUPING SETS ((merchant, CASE WHEN first_known_is_flagged_flag_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN last_known_row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END), (merchant, CASE WHEN first_known_is_flagged_flag_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END), (merchant), ())
HAVING num_txn_types >= 1;
