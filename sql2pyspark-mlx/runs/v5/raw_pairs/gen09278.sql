SELECT plan, status, MIN(start_date) AS min_start_date, MAX(end_date) AS latest_end_date FROM subscriptions GROUP BY plan, status ORDER BY plan ASC;
