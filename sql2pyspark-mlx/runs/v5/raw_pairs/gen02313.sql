select id, txn_type, account_id from transactions where account_id in (8, 23, 29) union select id, txn_type, account_id from transactions where is_flagged = true
