SELECT customer_id, MAX(end_date) AS latest_end_date, SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_count FROM subscriptions GROUP BY customer_id;
