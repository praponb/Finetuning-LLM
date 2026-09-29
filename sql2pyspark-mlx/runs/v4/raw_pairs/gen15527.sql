select id, status, cast(amount as int) as amount_int from payments where order_id is not null and amount < 1570 order by amount_int desc;
