select status, plan, count(case when plan = 'premium' then 1 end) as premium_count
from subscriptions
where start_date < '2025-04-01'
group by cube(status, plan)
