WITH scored AS (
    SELECT
        *,
        NTILE(3) OVER w_ordered AS amount_bucket,
        LAST_VALUE(merchant) IGNORE NULLS OVER w_ordered_2 AS last_known_merchant
    FROM transactions
    WHERE txn_type = 'DEBIT'
    WINDOW w_ordered AS (PARTITION BY merchant ORDER BY amount, id), w_ordered_2 AS (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    SELECT account_id
    FROM scored
    INTERSECT ALL
    SELECT account_id
    FROM transactions
    WHERE amount < 2100
),
verified AS (
    SELECT s.*
    FROM scored s
    JOIN remaining_keys k ON s.account_id = k.account_id
),
filtered AS (
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        merchant,
        is_flagged,
        amount_bucket,
        last_known_merchant,
        CASE WHEN is_flagged THEN 'Yes' ELSE 'No' END AS is_flagged_label
    FROM verified
    WHERE merchant IS NOT NULL
),
scored_transactions AS (
    SELECT
        id,
        account_id,
        last_known_merchant,
        is_flagged_label,
        MAX(amount_bucket) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_bucket_max_7d,
        MIN(amount_bucket) OVER (PARTITION BY account_id, txn_type) AS account_id_min_amount_bucket
    FROM filtered
)
SELECT
    p.status,
    COUNT(*) AS num_rows,
    ROUND(SUM(a.amount_bucket_max_7d), 2) AS total_amount_bucket_max_7d,
    SUM(a.account_id_min_amount_bucket) AS total_account_id_min_amount_bucket
FROM scored_transactions a
JOIN accounts p ON a.account_id = p.id
GROUP BY p.status;
