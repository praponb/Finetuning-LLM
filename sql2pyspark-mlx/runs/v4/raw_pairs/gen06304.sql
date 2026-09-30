select a.id, a.opened_date from accounts a where not exists (select 1 from transactions t where t.account_id = a.id and t.account_id in (16, 21, 24, 34)) order by a.opened_date desc
