select plan, sum(monthly_fee) as total_monthly_fee, sum(sum(monthly_fee)) over (order by plan) as cumulative_monthly_fee
from subscriptions
group by plan
order by plan
limit 10
