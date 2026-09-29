SELECT customer_id
FROM accounts
WHERE MONTH(opened_date) IN (1, 2, 3)
UNION
SELECT customer_id
FROM accounts
WHERE balance <= 39000
ORDER BY customer_id;
