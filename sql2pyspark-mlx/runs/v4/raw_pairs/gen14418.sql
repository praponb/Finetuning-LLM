select plan, round(avg(monthly_fee), 2) as avg_monthly_fee from subscriptions group by plan order by avg_monthly_fee, plan limit 20
