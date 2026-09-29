select merchant, is_flagged, account_id, date_trunc('week', txn_time) as txn_time_period from transactions;
