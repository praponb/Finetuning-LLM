SELECT
    id,
    plan,
    monthly_fee,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 3) AS monthly_fee_share
FROM subscriptions
ORDER BY 1 DESC;
