SELECT *
FROM accounts
WHERE customer_id IN (8, 17, 22, 30) AND opened_date BETWEEN '2023-10-01' AND '2024-11-10'
ORDER BY 5, id
LIMIT 5;
