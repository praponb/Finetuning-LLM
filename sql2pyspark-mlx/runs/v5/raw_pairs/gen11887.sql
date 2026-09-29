WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(txn_type) AS txn_type_length,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        COALESCE(merchant, 'UNKNOWN') AS merchant_filled
    FROM transactions
    WHERE merchant IS DISTINCT FROM 'Grab'
      AND merchant IS NOT NULL
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN txn_time - LAG(txn_time) OVER (PARTITION BY account_id ORDER BY txn_time, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM valid
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        txn_type_length,
        is_flagged_flag,
        merchant_filled,
        is_new_burst,
        island_id,
        CAST(txn_time AS DATE) AS txn_time_day,
        txn_type_length * is_flagged_flag AS txn_type_length_times_is_flagged_flag
    FROM sessionized
    WHERE account_id IN (2, 12)
),
with_dims AS (
    -- Attach accounts attributes
    SELECT
        src.id,
        src.account_id,
        src.txn_type,
        src.txn_type_length,
        src.is_flagged_flag,
        src.merchant_filled,
        src.is_new_burst,
        src.island_id,
        d.account_type,
        d.status
    FROM filtered src
    LEFT JOIN accounts d
        ON src.account_id = d.id
),
filtered_transactions AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, is_new_burst * is_flagged_flag AS is_new_burst_times_is_flagged_flag
    FROM with_dims
    WHERE account_type IS DISTINCT FROM 'CHECKING'
      AND account_type IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(island_id) OVER (PARTITION BY account_id ORDER BY txn_type_length, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS island_id_avg_3_prior,
        CUME_DIST() OVER (PARTITION BY status ORDER BY txn_type_length DESC NULLS LAST) AS txn_type_length_cume_dist
    FROM filtered_transactions
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        txn_type,
        txn_type_length,
        is_flagged_flag,
        is_new_burst,
        island_id,
        is_new_burst_times_is_flagged_flag,
        island_id_avg_3_prior
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY is_new_burst DESC NULLS LAST, id) AS row_num
        FROM metrics
    ) ranked
    WHERE row_num = 1
)
-- Grouped metrics
SELECT
    island_id,
    PERCENTILE_APPROX(txn_type_length, 0.5) AS txn_type_length_p50,
    COUNT(*) AS row_count
FROM deduped
GROUP BY island_id
HAVING txn_type_length_p50 < 1000000;
