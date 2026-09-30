select plan, min(start_date) as min_start_date, sum(case when plan = 'enterprise' then 1 else 0 end) as enterprise_count
from subscriptions
where plan not in ('premium', 'enterprise', 'standard') and customer_id = 30
group by plan
order by min_start_date;
