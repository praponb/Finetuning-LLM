select t.id, t.is_flagged, t.amount, t.txn_type from transactions t where t.account_id in (select id from accounts where status = 'FROZEN') order by t.id desc limit 1;
