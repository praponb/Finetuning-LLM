select order_id, count(amount) as amount_count from payments group by order_id having amount_count < 2 order by order_id desc;
