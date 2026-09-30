select status, min(start_date) as min_start_date from subscriptions group by status order by min_start_date
