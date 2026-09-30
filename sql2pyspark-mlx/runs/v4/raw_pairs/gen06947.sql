SELECT id, start_date, monthly_fee, DAYOFWEEK(end_date) AS end_date_dayofweek FROM subscriptions WHERE YEAR(end_date) >= 2024;
