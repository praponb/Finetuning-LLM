select distinct status, plan
from subscriptions
where end_date between '2023-06-01' and '2024-06-01'
order by plan, status
limit 5
