SELECT status, MAX(opened_date) AS last_opened_date
FROM accounts
GROUP BY status
ORDER BY status ASC;
