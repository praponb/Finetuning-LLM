SELECT customer_id FROM accounts EXCEPT SELECT customer_id FROM accounts WHERE status = 'OPEN';
