select id, txn_time from transactions where txn_time < '2024-03-01' order by txn_time, id limit 5
