select distinct status
from accounts
where status not in ('FROZEN', 'OPEN')
order by status
limit 50
