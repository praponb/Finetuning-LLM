select id, txn_time, lower(txn_type) as txn_type_lower
from transactions
order by id
limit 1
