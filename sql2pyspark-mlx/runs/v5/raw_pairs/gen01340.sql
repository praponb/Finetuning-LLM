select order_id, status, amount from payments where to_date(paid_at) >= '2023-04-01' and (order_id is not null or amount between 760 and 980) order by order_id
