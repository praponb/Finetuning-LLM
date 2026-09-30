WITH with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY customer_id ORDER BY monthly_fee NULLS FIRST) AS monthly_fee_cume_dist,
        LAG(monthly_fee, 2, 0) OVER (PARTITION BY customer_id, plan ORDER BY monthly_fee, id) AS prev_monthly_fee
    FROM subscriptions
)
SELECT
    s.id,
    s.customer_id,
    s.plan,
    s.start_date,
    s.monthly_fee,
    s.monthly_fee_cume_dist,
    s.prev_monthly_fee,
    (SELECT AVG(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS avg_account_balance
FROM with_windows s;
