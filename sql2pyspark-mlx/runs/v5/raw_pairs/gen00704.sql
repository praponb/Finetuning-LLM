select hour(txn_time) as txn_hour, max(txn_time) as last_txn_time from transactions where account_id in (2, 14) and (is_flagged or to_date(txn_time) < '2023-04-21') group by hour(txn_time)
