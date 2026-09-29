select id, txn_time from transactions where txn_time < '2025-01-19' or txn_type = 'REFUND' order by id desc
