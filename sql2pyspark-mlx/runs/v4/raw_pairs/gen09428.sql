select s.* from customers s where not exists (select 1 from sessions c where c.customer_id = s.id and c.country in ('JP', 'SG'))
