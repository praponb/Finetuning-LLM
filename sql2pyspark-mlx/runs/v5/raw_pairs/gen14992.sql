select id, opened_date, rank() over (order by opened_date) as opened_date_rank
from accounts
where account_type = 'CREDIT';
