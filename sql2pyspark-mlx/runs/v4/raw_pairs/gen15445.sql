select end_date
from subscriptions
where start_date between '2023-09-25' and '2025-03-01'
intersect
select opened_date
from accounts
order by end_date
