SELECT account_type, CASE WHEN account_type IN ('SAVINGS', 'CHECKING') THEN 'priority' ELSE 'other' END AS account_type_group, MIN(opened_date) AS min_opened_date
FROM accounts
GROUP BY 1, 2
ORDER BY min_opened_date DESC;
