select id, opened_date, lead(opened_date, 1) over (order by opened_date, id) as next_opened_date
from accounts
order by opened_date
