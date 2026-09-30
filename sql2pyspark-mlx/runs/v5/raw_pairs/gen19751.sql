select month(start_date) start_month, sum(monthly_fee) total_monthly_fee, round(sum(monthly_fee) * 100.0 / sum(sum(monthly_fee)) over (), 2) as pct_of_total
from subscriptions
where customer_id = 25
group by month(start_date)
