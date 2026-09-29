WITH base AS (
    SELECT *
    FROM transactions
    WHERE merchant IN ('Shell', '7-Eleven')
),
transactions_stats AS (
    SELECT CASE WHEN amount >= 3600 THEN 'high' WHEN amount >= 1400 THEN 'mid' ELSE 'low' END AS amount_bucket, AVG(amount) AS avg_amount, SORT_ARRAY(COLLECT_SET(txn_type)) AS txn_type_list
    FROM base
    GROUP BY CASE WHEN amount >= 3600 THEN 'high' WHEN amount >= 1400 THEN 'mid' ELSE 'low' END
)
SELECT amount_bucket, avg_amount, txn_type_list
FROM transactions_stats
WHERE avg_amount >= 3900
ORDER BY amount_bucket, avg_amount DESC;
