select status, method from payments where method = 'paypal' union all select status, method from payments where order_id in (1, 30);
