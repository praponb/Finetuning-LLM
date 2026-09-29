select id, method, status from payments where order_id is not null and amount between 640 and 1110 and amount <= 550 order by method asc, status desc, id asc limit 3
