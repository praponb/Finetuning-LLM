select a.id, a.opened_date, a.account_type from accounts a where not exists (select 1 from transactions t where t.account_id = a.id);
