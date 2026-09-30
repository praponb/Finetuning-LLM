WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM accounts a
    INNER JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.opened_date BETWEEN sub.start_date AND sub.end_date
)
-- Multi-level aggregation
SELECT
    customer_id,
    APPROX_PERCENTILE(subscription_id, 0.9) AS subscription_id_p90,
    ROUND(SUM(balance) / NULLIF(COUNT(CASE WHEN plan = 'standard' THEN 1 END), 0), 2) AS balance_per_standard,
    AVG(monthly_fee) FILTER (WHERE plan = 'enterprise') AS enterprise_avg_monthly_fee
FROM with_window
GROUP BY customer_id;
