select * from (select merchant, txn_type, count(case when merchant = 'Grab' then 1 end) grab_count from transactions where account_id = 29 group by merchant, txn_type) sub where grab_count >= 4;
