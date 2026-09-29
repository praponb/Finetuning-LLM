SELECT status, customer_id, ADD_MONTHS(end_date, 3) AS end_date_plus_months
FROM subscriptions
WHERE start_date BETWEEN '2024-01-09' AND '2024-06-01' AND customer_id = 7 AND monthly_fee BETWEEN 120 AND 160
ORDER BY customer_id
