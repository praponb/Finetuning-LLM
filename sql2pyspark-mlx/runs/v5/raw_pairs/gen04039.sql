SELECT CASE WHEN txn_type IN ('FEE', 'DEBIT') THEN 'priority' ELSE 'other' END AS txn_type_group, AVG(amount) AS avg_amount FROM transactions GROUP BY 1 HAVING AVG(amount) > 2000 ORDER BY 2
