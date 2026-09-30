select id, account_id, txn_time, is_flagged from (select *, row_number() over (partition by account_id order by txn_time desc, id) row_num from transactions) r where row_num <= 1
