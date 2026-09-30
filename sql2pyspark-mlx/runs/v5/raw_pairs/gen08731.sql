WITH customer_totals AS (
    SELECT customer_id, ROUND(STDDEV(monthly_fee), 2) AS stddev_monthly_fee, MIN(end_date) AS min_end_date
    FROM subscriptions
    GROUP BY customer_id
)
SELECT c.age, ct.stddev_monthly_fee, ct.min_end_date
FROM customers c
LEFT JOIN customer_totals ct ON c.id = ct.customer_id;
