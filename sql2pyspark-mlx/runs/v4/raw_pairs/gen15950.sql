WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        NTILE(3) OVER (PARTITION BY merchant, account_id ORDER BY amount, id) AS amount_bucket,
        RANK() OVER (PARTITION BY merchant ORDER BY amount NULLS LAST) AS amount_rank
    FROM transactions
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN amount_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_rank_tier,
    MIN(txn_time) AS first_txn_time,
    COUNT(DISTINCT id) AS distinct_ids,
    AVG(amount_rank) AS avg_amount_rank,
    MIN(txn_time) AS first_txn_time_2
FROM enriched
GROUP BY 1;
