select id, store_id, order_date from orders where status = 'PENDING' or status <> 'RETURNED';
