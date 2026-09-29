WITH with_windows AS (
    SELECT
        *,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY start_date ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS monthly_fee_avg_2,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee DESC NULLS LAST) AS monthly_fee_dense_rank,
        AVG(monthly_fee) OVER (PARTITION BY status ORDER BY start_date ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS monthly_fee_avg_3_prior,
        ROW_NUMBER() OVER (ORDER BY start_date ASC, id ASC) AS seq_no,
        RANK() OVER (PARTITION BY customer_id ORDER BY monthly_fee ASC) AS monthly_fee_rank
    FROM subscriptions
)
SELECT
    seq_no,
    CASE WHEN monthly_fee_avg_2 >= 262 THEN 'HIGH' ELSE 'LOW' END AS monthly_fee_avg_2_tier,
    COUNT_IF(status = 'PAUSED') AS paused_events,
    COUNT(*) AS row_count
FROM with_windows
GROUP BY GROUPING SETS ((seq_no, CASE WHEN monthly_fee_avg_2 >= 262 THEN 'HIGH' ELSE 'LOW' END), (seq_no), ())
HAVING COUNT_IF(status = 'PAUSED') >= 1;
