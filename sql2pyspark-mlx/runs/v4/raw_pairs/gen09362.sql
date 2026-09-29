select status, sum(monthly_fee) as total_monthly_fee, round(sum(monthly_fee) * 100.0 / sum(sum(monthly_fee)) over (), 2) as pct_of_total
from subscriptions
group by status
order by total_monthly_fee, status
limit 50
