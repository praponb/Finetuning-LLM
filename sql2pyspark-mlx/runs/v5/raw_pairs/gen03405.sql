SELECT *, LAST_DAY(start_date) AS start_date_month_end
FROM subscriptions
ORDER BY start_date_month_end ASC;
