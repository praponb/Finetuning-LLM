select id, amount, method, status from payments where order_id = 33 and method in ('bank_transfer', 'cash', 'card')
