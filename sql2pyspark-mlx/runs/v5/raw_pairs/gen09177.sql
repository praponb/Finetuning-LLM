select s.* from customers s where exists (select 1 from sessions x where x.customer_id = s.id and x.started_at < s.signup_date)
