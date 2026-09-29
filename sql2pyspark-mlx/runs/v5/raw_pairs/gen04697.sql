SELECT customer_id, opened_date, balance
FROM accounts
WHERE balance < 43000 AND balance <= 40000 AND balance BETWEEN 28000 AND 39000;
