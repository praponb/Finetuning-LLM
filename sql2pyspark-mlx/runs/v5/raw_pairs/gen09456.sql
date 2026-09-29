select plan, count(monthly_fee) as monthly_fee_count, min(start_date) as min_start_date
from subscriptions
group by plan
having count(monthly_fee) < 2;
