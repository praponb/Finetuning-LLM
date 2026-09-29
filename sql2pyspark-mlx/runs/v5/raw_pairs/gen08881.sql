select id, txn_time, txn_type, datediff(current_date(), txn_time) as days_since_txn_time, upper(txn_type) as txn_type_upper from transactions where is_flagged = false and txn_type = 'FEE'
