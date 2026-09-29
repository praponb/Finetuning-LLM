SELECT opened_date, customer_id FROM accounts WHERE customer_id IN (11, 19) INTERSECT SELECT opened_date, customer_id FROM accounts WHERE customer_id = 31;
