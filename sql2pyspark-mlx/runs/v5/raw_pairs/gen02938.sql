SELECT id, opened_date FROM accounts WHERE customer_id IN (8, 27) INTERSECT SELECT id, opened_date FROM accounts WHERE MONTH(opened_date) IN (1, 6, 9) ORDER BY id
