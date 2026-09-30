WITH remaining_keys AS (
    SELECT customer_id, id
    FROM subscriptions
    EXCEPT ALL
    SELECT customer_id, id
    FROM subscriptions
    WHERE monthly_fee BETWEEN 140 AND 160
),
kept_rows AS (
    SELECT s.*
    FROM subscriptions s
    JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
SELECT
    s.customer_id,
    s.plan,
    s.start_date,
    s.end_date,
    s.monthly_fee,
    s.status,
    (SELECT AVG(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS avg_order_amount
FROM kept_rows s
ORDER BY status DESC NULLS LAST
