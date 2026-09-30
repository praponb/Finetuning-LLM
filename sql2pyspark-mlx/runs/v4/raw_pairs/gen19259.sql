WITH remaining_keys AS (
    SELECT customer_id, id
    FROM subscriptions
    EXCEPT
    SELECT customer_id, id
    FROM subscriptions
    WHERE monthly_fee > 90
),
reconciled AS (
    SELECT s.*
    FROM subscriptions s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id AND s.id = k.id
)
SELECT
    s.id,
    s.customer_id,
    s.start_date,
    s.end_date,
    s.monthly_fee,
    s.status,
    (SELECT MAX(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM reconciled s
ORDER BY customer_id DESC NULLS LAST, end_date
