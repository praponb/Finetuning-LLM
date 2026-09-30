SELECT account_type, status, SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance FROM accounts WHERE status = 'FROZEN' AND opened_date <= '2023-06-24' GROUP BY 1, 2 ORDER BY 1;
