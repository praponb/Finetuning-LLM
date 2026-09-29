select a.id, a.account_type, a.opened_date, a.status from accounts a where exists (select 1 from transactions t where t.account_id = a.id);
