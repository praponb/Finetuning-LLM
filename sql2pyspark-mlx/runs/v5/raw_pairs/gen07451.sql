select amount, date_trunc('month', txn_time) as txn_time_period from transactions order by amount
