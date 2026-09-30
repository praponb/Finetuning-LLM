WITH metrics AS (
    SELECT
        id,
        customer_id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        SUM(monthly_fee) OVER (PARTITION BY status, customer_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_monthly_fee,
        ROW_NUMBER() OVER w_ordered AS event_seq
    FROM subscriptions
    WINDOW w_ordered AS (PARTITION BY plan, status ORDER BY start_date, id)
)
SELECT
    CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_tier,
    ROUND(SUM(monthly_fee) / NULLIF(COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END), 0), 2) AS monthly_fee_per_active,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    COUNT(*) AS row_count
FROM metrics
GROUP BY CASE WHEN event_seq >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(DISTINCT customer_id) >= 1
ORDER BY row_count
