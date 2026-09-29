select id, method, order_id from payments where amount > 1560 except select id, method, order_id from payments where status != 'FAILED' order by order_id
