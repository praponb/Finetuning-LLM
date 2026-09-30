SELECT
    id,
    plan,
    monthly_fee,
    customer_id,
    FIRST_VALUE(customer_id) OVER (PARTITION BY plan ORDER BY monthly_fee DESC, id) AS first_customer_id
FROM subscriptions;
