select plan, sum(monthly_fee) as total_monthly_fee, dense_rank() over (order by sum(monthly_fee) desc) as rnk from subscriptions where year(end_date) = 2025 group by plan
