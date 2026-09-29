select a.id, a.status from accounts a where exists (select 1 from transactions t where t.account_id = a.id) order by a.status, id;
