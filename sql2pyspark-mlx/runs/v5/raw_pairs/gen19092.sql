WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        is_flagged,
        AVG(amount) OVER w_running AS amount_avg_6,
        SUM(amount) OVER (PARTITION BY account_id, txn_type ORDER BY CAST(txn_time AS BIGINT) ASC RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_sum_30d
    FROM transactions
    WHERE txn_type NOT IN ('REFUND', 'DEBIT', 'FEE')
    WINDOW w_running AS (PARTITION BY account_id ORDER BY txn_time ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    CASE WHEN amount_avg_6 >= 3323 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_avg_6_tier,
    ROUND(AVG(amount_avg_6), 2) AS avg_amount_avg_6,
    PERCENTILE_APPROX(amount_sum_30d, ARRAY(0.25, 0.5, 0.75)) AS amount_sum_30d_quartiles,
    PERCENTILE_APPROX(amount_avg_6, 0.5) AS amount_avg_6_p50
FROM with_windows
GROUP BY amount_avg_6_tier
ORDER BY avg_amount_avg_6 DESC NULLS LAST;
