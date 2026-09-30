select txn_time, date_trunc('MONTH', txn_time) as txn_time_period
from transactions
where account_id = 12 and account_id = 10;
