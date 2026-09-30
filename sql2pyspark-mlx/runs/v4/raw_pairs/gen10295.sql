select id
from transactions
intersect
select id
from transactions
where account_id in (27, 29)
