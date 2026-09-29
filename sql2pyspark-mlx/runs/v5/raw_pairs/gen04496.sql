WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN amount >= 3100 THEN 'High' WHEN amount >= 1400 THEN 'Medium' ELSE 'Low' END amount_tier,
        CASE WHEN txn_type IN ('CREDIT', 'REFUND') THEN 'Tier 1' ELSE 'Other' END txn_type_group
    FROM transactions
    WHERE txn_time >= DATE '2023-08-26'
      AND merchant IS DISTINCT FROM '7-Eleven'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT merchant
    FROM valid
    INTERSECT ALL
    SELECT merchant
    FROM transactions
    WHERE amount < 1700
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.merchant = k.merchant
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT merchant, txn_type
    FROM verified
    INTERSECT ALL
    SELECT merchant, txn_type
    FROM transactions
    WHERE account_id IN (31, 32)
),
verified_transactions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM verified s
    INNER JOIN remaining_keys k ON s.merchant = k.merchant AND s.txn_type = k.txn_type
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.*,
        b.status,
        b.opened_date,
        b.balance
    FROM verified_transactions src
    LEFT JOIN accounts b
        ON src.account_id = b.id
    WHERE b.account_type != 'CHECKING'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY status ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) amount_sum_7d,
        AVG(amount) OVER (ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) amount_avg_90d
    FROM with_dims
),
daily AS (
    -- Multi-level aggregation
    SELECT
        txn_type,
        COUNT(*) AS row_count,
        SIZE(COLLECT_SET(status)) AS num_status,
        PERCENTILE_APPROX(amount_avg_90d, ARRAY(0.25, 0.5, 0.75)) AS amount_avg_90d_quartiles,
        SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM enriched
    GROUP BY txn_type
)
SELECT
    txn_type,
    row_count,
    amount_avg_90d_quartiles
FROM daily
ORDER BY row_count NULLS FIRST;
