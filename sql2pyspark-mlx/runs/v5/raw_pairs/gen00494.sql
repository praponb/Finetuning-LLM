select id, order_id, amount, ntile(2) over (partition by order_id order by amount desc, id) as quartile from payments where hour(paid_at) between 8 and 15
