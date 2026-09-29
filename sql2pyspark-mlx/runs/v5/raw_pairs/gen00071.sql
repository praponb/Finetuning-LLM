WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY start_date DESC, id) AS rank_in_group
    FROM subscriptions
    QUALIFY rank_in_group = 1
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.end_date,
    s.monthly_fee,
    (SELECT MIN(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS min_subscription_monthly_fee
FROM ranked s
