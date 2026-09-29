select id, customer_id, balance from (select *, row_number() over (partition by customer_id order by balance, id) as row_num from accounts) as t where row_num <= 5 order by customer_id desc, id desc;
