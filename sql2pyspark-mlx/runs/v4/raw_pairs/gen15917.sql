SELECT plan, start_date, LAST_DAY(end_date) AS end_date_month_end, ROUND(monthly_fee, 2) AS monthly_fee_rounded FROM subscriptions WHERE customer_id IN (3, 6, 23, 24) ORDER BY start_date
