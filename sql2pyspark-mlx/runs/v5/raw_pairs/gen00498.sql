select t.id, t.is_flagged from transactions t where t.txn_type in ('FEE', 'REFUND', 'CREDIT') and t.account_id in (select id from accounts where status in ('OPEN', 'CLOSED', 'FROZEN'));
