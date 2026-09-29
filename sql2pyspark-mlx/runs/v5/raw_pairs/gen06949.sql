WITH base AS (
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        LOWER(merchant) AS merchant_lower,
        COALESCE(merchant, 'N/A') AS merchant_clean
    FROM transactions
    WHERE txn_time >= TIMESTAMP '2023-11-02 06:00:00'
      AND merchant IS DISTINCT FROM '7-Eleven'
),
kept_keys AS (
    SELECT merchant
    FROM base
    EXCEPT
    SELECT merchant
    FROM transactions
    WHERE account_id IN (39, 40)
),
reconciled AS (
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.merchant = k.merchant
),
scored AS (
    SELECT
        txn_type,
        txn_time,
        CUME_DIST() OVER (PARTITION BY merchant ORDER BY amount DESC NULLS LAST) AS amount_cume_dist,
        SUM(is_flagged_flag) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS is_flagged_flag_sum_7d
    FROM reconciled
),
filtered AS (
    SELECT
        *,
        DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_time_label,
        CAST(amount_cume_dist AS INT) AS amount_cume_dist_int
    FROM scored
    WHERE txn_time >= TIMESTAMP '2023-05-06 12:00:00'
),
valid AS (
    SELECT *, GREATEST(is_flagged_flag_sum_7d, amount_cume_dist) AS greatest_is_flagged_flag_sum_7d_amount_cume_dist
    FROM filtered
    WHERE txn_time >= TO_DATE('2023-05-02')
      AND txn_type != 'REFUND'
),
totals AS (
    SELECT
        CASE WHEN greatest_is_flagged_flag_sum_7d_amount_cume_dist >= 72 THEN 'HIGH' ELSE 'LOW' END AS greatest_is_flagged_flag_sum_7d_amount_cume_dist_tier,
        txn_type,
        COUNT(DISTINCT txn_type) AS distinct_txn_types,
        (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
        PERCENTILE_APPROX(greatest_is_flagged_flag_sum_7d_amount_cume_dist, 0.95) AS greatest_is_flagged_flag_sum_7d_amount_cume_dist_p95,
        COUNT_IF(txn_type = 'REFUND') AS refund_events
    FROM valid
    GROUP BY ROLLUP (CASE WHEN greatest_is_flagged_flag_sum_7d_amount_cume_dist >= 72 THEN 'HIGH' ELSE 'LOW' END, txn_type)
    HAVING (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 > 0
)
SELECT
    greatest_is_flagged_flag_sum_7d_amount_cume_dist_tier,
    txn_type,
    distinct_txn_types,
    duration_min,
    greatest_is_flagged_flag_sum_7d_amount_cume_dist_p95
FROM totals
ORDER BY distinct_txn_types;
