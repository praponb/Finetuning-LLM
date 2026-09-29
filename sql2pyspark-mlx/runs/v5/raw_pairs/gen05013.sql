SELECT id, plan, start_date, monthly_fee, LAST_DAY(start_date) AS start_date_month_end FROM subscriptions WHERE plan IN ('basic', 'standard', 'premium') AND monthly_fee BETWEEN 170 AND 230;
