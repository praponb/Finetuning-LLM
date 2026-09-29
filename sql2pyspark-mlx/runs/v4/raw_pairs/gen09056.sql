SELECT *, DATE_FORMAT(start_date, 'MMM yyyy') AS start_date_label
FROM subscriptions
WHERE NOT (customer_id IN (16, 19, 21, 27) AND customer_id IN (9, 20))
ORDER BY monthly_fee DESC, status DESC
