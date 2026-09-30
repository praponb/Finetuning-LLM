WITH filtered AS (
    SELECT
        id,
        txn_type,
        txn_time,
        is_flagged,
        CONCAT_WS('/', txn_type, merchant) txn_type_merchant_key,
        COALESCE(merchant, 'NONE') merchant_clean
    FROM transactions
    WHERE txn_time >= TO_TIMESTAMP('2023-05-05 01:00:00')
      AND txn_type = 'DEBIT'
),
with_prev AS (
    SELECT *, LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_txn_time
    FROM filtered
),
with_gaps AS (
    SELECT *, CASE WHEN prev_txn_time IS NULL OR txn_time > prev_txn_time + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_prev
),
sessions_transactions AS (
    SELECT
        id,
        txn_type,
        txn_time,
        is_flagged,
        txn_type_merchant_key,
        merchant_clean,
        SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
kept_keys AS (
    SELECT txn_type, id
    FROM sessions_transactions
    EXCEPT
    SELECT txn_type, id
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 12 AND 19
),
reconciled AS (
    SELECT s.*
    FROM sessions_transactions s
    INNER JOIN kept_keys k ON s.txn_type = k.txn_type AND s.id = k.id
),
enriched AS (
    SELECT
        txn_type,
        txn_time,
        is_flagged,
        txn_type_merchant_key,
        island_id,
        AVG(island_id) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) island_id_avg_7d,
        DENSE_RANK() OVER (ORDER BY island_id DESC) island_id_dense_rank
    FROM reconciled
)
SELECT
    txn_type,
    txn_type_merchant_key,
    island_id,
    island_id_dense_rank,
    CASE WHEN txn_type RLIKE '^(FEE|DEBIT)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS txn_type_zone
FROM enriched;
