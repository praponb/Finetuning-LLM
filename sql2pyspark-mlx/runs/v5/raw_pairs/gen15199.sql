select txn_type, id
from transactions
except all
select txn_type, id
from transactions
where txn_time >= '2025-02-12';
