SELECT customer_id, status FROM accounts EXCEPT SELECT customer_id, status FROM accounts WHERE balance < 78000
