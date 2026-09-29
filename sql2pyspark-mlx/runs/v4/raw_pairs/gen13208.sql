WITH subscriptions_by_customer AS (
    SELECT customer_id, MAX(monthly_fee) - MIN(monthly_fee) AS monthly_fee_range
    FROM subscriptions
    GROUP BY customer_id
)
SELECT
    c.id,
    c.country,
    c.age,
    sbc.monthly_fee_range
FROM customers c
LEFT OUTER JOIN subscriptions_by_customer sbc ON c.id = sbc.customer_id
WHERE sbc.monthly_fee_range > 31
ORDER BY c.id DESC, country
