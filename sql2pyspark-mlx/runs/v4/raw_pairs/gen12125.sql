select merchant, min(txn_time) as min_txn_time from transactions where account_id in (22, 31) or merchant in ('Grab', 'Starbucks', 'Lazada') group by 1 order by min_txn_time, merchant limit 5;
