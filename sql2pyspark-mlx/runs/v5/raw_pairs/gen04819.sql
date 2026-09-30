select id, paid_at, status, amount, rank() over (order by paid_at desc) as rnk, row_number() over (partition by status order by amount, id) as row_num from payments where order_id = 20;
