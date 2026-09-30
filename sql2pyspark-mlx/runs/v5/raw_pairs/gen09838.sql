SELECT
    id,
    status,
    monthly_fee,
    plan,
    DENSE_RANK() OVER (PARTITION BY status ORDER BY monthly_fee DESC) AS monthly_fee_dense_rank,
    ROUND(monthly_fee / SUM(monthly_fee) OVER (PARTITION BY plan), 3) AS monthly_fee_share
FROM subscriptions
