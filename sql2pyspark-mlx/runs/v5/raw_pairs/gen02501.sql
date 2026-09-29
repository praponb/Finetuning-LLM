select id, opened_date, lower(account_type) as account_type_lower
from accounts
where customer_id in (10, 17, 23, 31)
order by opened_date asc;
