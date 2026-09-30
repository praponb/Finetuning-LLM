WITH RankedSubscriptions AS (
    SELECT
        id,
        plan,
        monthly_fee,
        status,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY monthly_fee) AS rn
    FROM subscriptions
)
SELECT *
FROM RankedSubscriptions
WHERE rn <= 5;
