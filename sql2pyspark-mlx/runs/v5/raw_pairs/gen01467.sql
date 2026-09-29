select id, customer_id, start_date, status, round(monthly_fee * 100, 2) as adjusted_monthly_fee from subscriptions where customer_id = 26 order by start_date asc;
