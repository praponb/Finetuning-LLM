WITH with_windows AS (
    SELECT
        *,
        COUNT(amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_count_7d,
        RANK() OVER w_running AS amount_rank
    FROM transactions
    WINDOW w_running AS (ORDER BY amount DESC NULLS FIRST)
)
SELECT
    merchant,
    account_id,
    CASE WHEN amount >= 4400 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_tier,
    APPROX_PERCENTILE(amount_rank, ARRAY(0.25, 0.5, 0.75)) AS amount_rank_quartiles,
    COUNT(DISTINCT txn_type) AS distinct_txn_types,
    STDDEV_SAMP(amount) AS amount_stddev,
    COUNT(*) FILTER (WHERE merchant = 'Starbucks') AS starbucks_rows,
    AVG(amount_count_7d) FILTER (WHERE merchant = 'Starbucks') AS starbucks_avg_amount_count_7d
FROM with_windows
GROUP BY merchant, account_id, CASE WHEN amount >= 4400 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY amount_stddev DESC NULLS LAST;
