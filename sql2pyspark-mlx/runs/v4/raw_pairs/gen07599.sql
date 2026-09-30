select customer_id, account_type
from accounts
where opened_date >= '2025-05-01'
union
select customer_id, account_type
from accounts
where account_type != 'SAVINGS';
