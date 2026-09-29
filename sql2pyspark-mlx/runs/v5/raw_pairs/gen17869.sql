select id, customer_id, device, started_at from (select *, row_number() over (partition by customer_id order by started_at, id) as row_num from sessions) t where row_num = 1
