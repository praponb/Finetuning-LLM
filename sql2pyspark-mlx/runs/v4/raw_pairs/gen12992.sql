SELECT *
FROM (
    SELECT plan, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    GROUP BY plan
) AS s
WHERE monthly_fee_range < 24
ORDER BY plan
LIMIT 50
