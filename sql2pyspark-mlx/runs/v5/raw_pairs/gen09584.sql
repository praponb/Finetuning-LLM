select s.* from accounts s where not exists (select 1 from transactions x where x.account_id = s.id and not x.is_flagged)
