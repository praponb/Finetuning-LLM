WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount NULLS FIRST, id) AS dedup_rank
        FROM transactions
    ) x
    WHERE dedup_rank = 1
)
SELECT
    CASE WHEN amount >= 3600 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    MAX(amount) AS max_amount,
    COUNT(DISTINCT id) AS distinct_ids,
    MIN(amount) AS min_amount,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN amount >= 3600 THEN 'HIGH' ELSE 'LOW' END ORDER BY MIN(amount) DESC) AS min_amount_dense_rank
FROM deduped
GROUP BY amount_tier, activity_month
ORDER BY amount_tier DESC
