WITH kept_keys AS (
    SELECT status
    FROM subscriptions
    EXCEPT DISTINCT
    SELECT status
    FROM subscriptions
    WHERE start_date < '2024-10-01'
),
reconciled AS (
    SELECT s.*
    FROM subscriptions s
    JOIN kept_keys k ON s.status = k.status
)
SELECT
    s.customer_id,
    s.start_date,
    s.end_date,
    s.monthly_fee,
    s.status,
    (SELECT SUM(x.shipping_fee) FROM orders x WHERE x.customer_id = s.customer_id) AS sum_order_shipping_fee
FROM reconciled s
ORDER BY sum_order_shipping_fee NULLS LAST, end_date DESC
