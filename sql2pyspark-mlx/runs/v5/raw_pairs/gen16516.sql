SELECT status, COUNT(DISTINCT balance) unique_statuss
FROM accounts
WHERE opened_date > '2024-05-01' AND status IN ('CLOSED', 'OPEN', 'FROZEN')
GROUP BY 1
