select * from (select id, account_id, txn_time, is_flagged, row_number() over (partition by account_id order by txn_time desc, id) as rn from transactions where amount < 1600) as t where rn = 1;
