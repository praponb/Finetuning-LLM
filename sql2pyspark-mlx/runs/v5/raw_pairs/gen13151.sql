select id, store_id, order_date, amount, amount - lag(amount, 1) over (partition by store_id order by order_date, id) as amount_change from orders order by order_date, amount desc;
