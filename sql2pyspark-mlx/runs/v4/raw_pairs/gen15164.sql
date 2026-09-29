SELECT
    id,
    customer_id,
    monthly_fee,
    plan,
    end_date,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY customer_id), 3) AS monthly_fee_share,
    RANK() OVER (PARTITION BY plan ORDER BY end_date) AS rnk
FROM subscriptions;
