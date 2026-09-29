SELECT monthly_fee, ADD_MONTHS(start_date, 1) AS start_date_plus_months FROM subscriptions WHERE start_date <= '2024-02-28' ORDER BY start_date_plus_months DESC;
