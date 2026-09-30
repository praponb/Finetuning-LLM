WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY merchant), 4) amount_share,
        ROW_NUMBER() OVER w_recent event_seq,
        CUME_DIST() OVER (PARTITION BY account_id ORDER BY amount DESC NULLS LAST) amount_cume_dist
    FROM transactions
    WINDOW w_recent AS (PARTITION BY account_id, merchant ORDER BY amount, id)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        amount_share,
        event_seq,
        amount_cume_dist,
        CAST(is_flagged AS INT) is_flagged_flag
    FROM with_windows
    WHERE txn_time > TO_TIMESTAMP('2023-11-02 01:00:00')
      AND merchant IS NOT NULL
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
        amount_share,
        event_seq,
        amount_cume_dist,
        is_flagged_flag,
        SUM(amount) OVER (PARTITION BY merchant) merchant_sum_amount,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY account_id), 4) event_seq_share
    FROM valid
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY account_id, txn_type ORDER BY amount) amount_cume_dist_2,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY event_seq DESC, id) row_seq,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount_share, id) event_seq_2
    FROM scored
)
-- Multi-level aggregation
SELECT
    CASE WHEN merchant_sum_amount >= 21213 THEN 'HIGH' ELSE 'LOW' END AS merchant_sum_amount_tier,
    CASE WHEN row_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
    CASE WHEN amount_cume_dist >= 0.81 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_cume_dist_tier,
    COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count,
    COUNT_IF(merchant = 'Amazon') AS amazon_events,
    COUNT(CASE WHEN merchant = 'Lazada' THEN 1 END) AS lazada_count
FROM metrics
GROUP BY CASE WHEN merchant_sum_amount >= 21213 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN row_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN amount_cume_dist >= 0.81 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) >= 1
ORDER BY row_seq_tier DESC
