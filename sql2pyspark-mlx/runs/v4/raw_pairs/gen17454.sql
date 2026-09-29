WITH deduped AS (
    SELECT *
    FROM transactions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount NULLS FIRST, id) = 1
)
SELECT
    CASE WHEN amount >= 3300 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    DATE_TRUNC('YEAR', txn_time) AS year_start,
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(AVG(amount), 2) AS avg_amount_2,
    COUNT(DISTINCT account_id) AS distinct_account_ids,
    MAX(amount) FILTER (WHERE txn_type = 'FEE') AS fee_max_amount,
    COUNT_IF(merchant = 'Lazada') AS lazada_events,
    MIN(amount) AS min_amount
FROM deduped
GROUP BY GROUPING SETS ((CASE WHEN amount >= 3300 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', txn_time), DATE_TRUNC('MONTH', txn_time)), (CASE WHEN amount >= 3300 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('YEAR', txn_time)))
HAVING lazada_events < 1
