select id, plan, start_date, monthly_fee, lag(monthly_fee, 1) over (partition by plan order by start_date, id) as prev_monthly_fee from subscriptions;
