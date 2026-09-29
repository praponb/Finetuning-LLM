WITH metrics AS (
    SELECT
        id,
        plan,
        start_date,
        end_date,
        monthly_fee,
        status,
        LAG(monthly_fee) OVER w_recent AS prev_monthly_fee,
        ROW_NUMBER() OVER w_recent AS row_seq,
        COUNT(monthly_fee) OVER (PARTITION BY customer_id) AS customer_id_count_monthly_fee
    FROM subscriptions
    WINDOW w_recent AS (PARTITION BY status ORDER BY monthly_fee, id)
)
SELECT
    id,
    plan,
    start_date,
    monthly_fee,
    status,
    prev_monthly_fee,
    row_seq,
    customer_id_count_monthly_fee,
    CASE WHEN plan RLIKE '^(premium|standard)$' THEN 'OTHER_ZONE' WHEN plan RLIKE '^(basic|enterprise)$' THEN 'GROUP_B' ELSE 'CORE' END AS plan_zone
FROM metrics
