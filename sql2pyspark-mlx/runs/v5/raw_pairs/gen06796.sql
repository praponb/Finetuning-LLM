SELECT dim.status, s.merchant, COUNT(*) AS cnt, SUM(s.amount) AS total_amount FROM transactions s LEFT JOIN accounts dim ON s.account_id = dim.id GROUP BY dim.status, s.merchant
