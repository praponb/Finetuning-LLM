select s.* from accounts s where exists (select 1 from transactions x where x.account_id = s.id and x.is_flagged)
