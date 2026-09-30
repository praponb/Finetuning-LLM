select id, merchant, account_id
from transactions
where merchant = 'Amazon'
intersect
select id, merchant, account_id
from transactions
where to_date(txn_time) >= '2024-04-22'
