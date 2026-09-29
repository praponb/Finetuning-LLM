select a.id, a.status, a.balance, a.account_type from accounts a where exists (select 1 from transactions t where t.account_id = a.id and t.amount <= 2000)
