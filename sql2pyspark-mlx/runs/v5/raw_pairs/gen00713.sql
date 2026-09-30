SELECT id, customer_id, end_date, status, LENGTH(status) AS status_length, ADD_MONTHS(end_date, 1) AS end_date_plus_months FROM subscriptions WHERE MONTH(start_date) IN (5, 8, 12);
