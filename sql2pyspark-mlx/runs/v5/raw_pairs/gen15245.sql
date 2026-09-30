select t.id, t.txn_time from transactions t where t.account_id in (6, 14, 22, 23) and t.account_id not in (select id from accounts where account_type != 'CHECKING') order by t.id, t.txn_time desc;
