select id, order_id, paid_at, amount, lag(amount, 1, 0) over (partition by order_id order by paid_at, id) as prev_amount from payments order by prev_amount desc, order_id desc
