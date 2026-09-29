select id, paid_at, amount, order_id, cast(amount as int) as amount_int, trim(status) as clean_status from payments order by order_id desc, clean_status asc;
