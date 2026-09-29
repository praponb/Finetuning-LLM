SELECT id, status
FROM accounts
WHERE opened_date BETWEEN '2024-01-01' AND '2025-03-01'
UNION
SELECT id, status
FROM accounts
WHERE balance >= 55000
ORDER BY id DESC
