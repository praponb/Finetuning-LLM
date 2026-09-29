select status, sum(monthly_fee) as total_monthly_fee, dense_rank() over (order by sum(monthly_fee) desc) as rnk from subscriptions group by status;
