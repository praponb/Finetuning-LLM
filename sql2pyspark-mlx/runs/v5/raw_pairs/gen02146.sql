WITH enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY monthly_fee DESC, id) AS row_seq,
        ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 4) AS monthly_fee_share,
        LAG(monthly_fee, 1) OVER w_ordered AS prev_monthly_fee
    FROM subscriptions
    WINDOW w_ordered AS (PARTITION BY customer_id, status ORDER BY start_date, id)
)
SELECT
    id,
    plan,
    start_date,
    end_date,
    monthly_fee,
    row_seq,
    prev_monthly_fee,
    CASE WHEN plan RLIKE '^(standard|premium)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS plan_zone
FROM enriched
ORDER BY id NULLS FIRST, plan_zone
LIMIT 5;
