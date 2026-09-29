select a.id, a.account_type, a.status, a.opened_date from accounts a where exists (select 1 from transactions t where t.account_id = a.id) order by a.status asc, a.opened_date asc
