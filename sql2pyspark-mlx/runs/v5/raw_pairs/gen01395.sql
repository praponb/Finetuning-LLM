SELECT DATE_TRUNC('MONTH', a.opened_date) AS opened_month, MAX(t.amount) - MIN(t.amount) AS amount_range
FROM transactions t
JOIN accounts a ON a.id = t.account_id
WHERE t.merchant NOT IN ('Grab', 'Lazada', 'Shell')
GROUP BY DATE_TRUNC('MONTH', a.opened_date);
