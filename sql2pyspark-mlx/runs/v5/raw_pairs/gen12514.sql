SELECT QUARTER(end_date) AS end_quarter, MIN(start_date) AS min_start_date FROM subscriptions GROUP BY QUARTER(end_date);
