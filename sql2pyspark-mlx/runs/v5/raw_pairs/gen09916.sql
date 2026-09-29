select status, min(start_date) as min_start_date from subscriptions where plan <> 'basic' and monthly_fee < 130 group by status order by min_start_date
