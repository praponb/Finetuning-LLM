select * from (select id, customer_id, amount, row_number() over (partition by customer_id order by amount desc, id) as pos from orders) as t where pos <= 3 order by customer_id desc;
