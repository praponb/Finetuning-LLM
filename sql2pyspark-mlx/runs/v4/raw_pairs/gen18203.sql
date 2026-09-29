WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END AS is_7_eleven,
        COALESCE(merchant, 'UNKNOWN') AS merchant_clean
    FROM transactions
    WHERE txn_time >= DATE '2024-02-08'
      AND merchant IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, amount / NULLIF(is_7_eleven, 0) AS amount_per_is_7_eleven
    FROM cleaned
    WHERE merchant IN ('7-Eleven', 'Grab')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(amount_per_is_7_eleven) OVER (PARTITION BY txn_type) AS txn_type_max_amount_per_is_7_eleven,
        NTILE(10) OVER (PARTITION BY account_id ORDER BY amount, id) AS amount_bucket,
        MAX(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_max_7d,
        AVG(amount_per_is_7_eleven) OVER (PARTITION BY txn_type ORDER BY amount_per_is_7_eleven, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_per_is_7_eleven_avg_2,
        NTILE(10) OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS amount_bucket_2
    FROM filtered
),
cleaned_transactions AS (
    -- Keep recent, valid rows
    SELECT *, IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM enriched
    WHERE txn_time >= DATE '2024-04-18'
      AND merchant IS NOT NULL
      AND is_flagged
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list,
    COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) AS debit_count,
    MAX(txn_time) AS last_seen
FROM cleaned_transactions
GROUP BY ROLLUP (DATE_TRUNC('QUARTER', txn_time))
HAVING COUNT(CASE WHEN txn_type = 'DEBIT' THEN 1 END) > 1;
