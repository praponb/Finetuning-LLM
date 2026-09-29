WITH base AS (
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        DATE_TRUNC('WEEK', txn_time) AS txn_time_period,
        COALESCE(merchant, 'DIRECT') AS merchant_filled
    FROM transactions
    WHERE txn_time > TO_DATE('2024-05-27')
      AND merchant IS NOT NULL
      AND amount >= 1500
),
kept_keys AS (
    SELECT txn_type, id
    FROM base
    INTERSECT
    SELECT txn_type, id
    FROM transactions
    WHERE merchant = 'Shell'
),
kept_rows AS (
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.txn_type = k.txn_type AND s.id = k.id
),
cleaned AS (
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        txn_time_period,
        merchant_filled,
        ROUND(amount * 1.1, 2) AS amount_adjusted,
        TO_DATE(txn_time_period) AS txn_time_period_date
    FROM kept_rows
    WHERE TO_DATE(txn_time) < '2024-11-01'
),
filtered AS (
    SELECT *, ROUND(amount_adjusted, 2) AS amount_adjusted_rounded
    FROM cleaned
    WHERE merchant IS NOT NULL
      AND HOUR(txn_time_period) BETWEEN 9 AND 13
)
SELECT
    merchant,
    COUNT(*) AS row_count,
    APPROX_PERCENTILE(amount_adjusted, 0.95) AS amount_adjusted_p95,
    SUM(amount_adjusted) FILTER (WHERE txn_type = 'REFUND') AS refund_total_amount_adjusted,
    COUNT(*) FILTER (WHERE merchant = 'Lazada') AS lazada_rows
FROM filtered
GROUP BY 1
ORDER BY merchant DESC;
