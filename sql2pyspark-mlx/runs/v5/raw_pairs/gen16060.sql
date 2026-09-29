select plan, round(avg(monthly_fee), 2) avg_monthly_fee, count(monthly_fee) monthly_fee_count, max(end_date) last_end_date from subscriptions group by plan order by last_end_date, 3 desc;
