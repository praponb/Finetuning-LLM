WITH cleaned AS (
    SELECT
        *,
        COALESCE(merchant, 'Unknown') AS merchant_filled,
        COALESCE(merchant, 'OTHER') AS resolved_merchant
    FROM transactions
    WHERE txn_time > TIMESTAMP '2024-02-07 16:00:00'
      AND merchant IS NOT NULL
),
matched_keys AS (
    SELECT txn_type
    FROM cleaned
    INTERSECT ALL
    SELECT txn_type
    FROM transactions
    WHERE merchant = 'Amazon'
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.txn_type = k.txn_type
),
matched_keys_2 AS (
    SELECT id, txn_type
    FROM reconciled
    EXCEPT
    SELECT id, txn_type
    FROM transactions
    WHERE amount <= 3500
),
verified AS (
    SELECT s.*
    FROM reconciled s
    INNER JOIN matched_keys_2 k ON s.id = k.id AND s.txn_type = k.txn_type
),
filtered AS (
    SELECT
        *,
        DATE_TRUNC('WEEK', txn_time) AS txn_time_period,
        SUBSTRING(merchant, 1, 4) AS merchant_prefix
    FROM verified
    WHERE merchant IS NOT NULL
      AND merchant IN ('Shell', 'Starbucks')
),
metrics AS (
    SELECT
        *,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share,
        MAX(amount) OVER (ORDER BY CAST(txn_time_period AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_max_7d,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share_2,
        ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 4) AS amount_share_3,
        SUM(amount) OVER (PARTITION BY txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_sum_30d
    FROM filtered
),
with_dims AS (
    SELECT s.*, d.opened_date
    FROM metrics s
    INNER JOIN accounts d
        ON s.account_id = d.id
),
cleaned_transactions AS (
    SELECT
        account_id,
        amount,
        merchant,
        is_flagged,
        merchant_filled,
        resolved_merchant,
        amount_max_7d,
        amount_share_2,
        amount_share_3,
        amount_sum_30d,
        CAST(amount AS INT) AS amount_int,
        COALESCE(merchant, 'DIRECT') AS resolved_merchant_2
    FROM with_dims
    WHERE NOT is_flagged
)
SELECT
    account_id,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list,
    COUNT(CASE WHEN merchant = 'Grab' THEN 1 END) AS grab_count,
    PERCENTILE_APPROX(amount_max_7d, 0.5) AS amount_max_7d_p50
FROM cleaned_transactions
GROUP BY account_id
