WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (ORDER BY amount DESC, id) AS amount_bucket,
        NTILE(10) OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS amount_bucket_2,
        AVG(amount) OVER (PARTITION BY account_id ORDER BY amount, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_avg_2,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior
    FROM transactions
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', txn_time) AS quarter_start,
    CASE WHEN amount_bucket_2 >= 8 THEN 'LARGE' ELSE 'SMALL' END AS amount_bucket_2_tier,
    DATE_TRUNC('MONTH', txn_time) AS activity_month,
    (UNIX_TIMESTAMP(MAX(txn_time)) - UNIX_TIMESTAMP(MIN(txn_time))) / 60 AS duration_min,
    SIZE(COLLECT_SET(merchant)) AS num_merchants
FROM enriched
GROUP BY DATE_TRUNC('QUARTER', txn_time), CASE WHEN amount_bucket_2 >= 8 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('MONTH', txn_time)
ORDER BY activity_month DESC NULLS FIRST;
