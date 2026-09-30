SELECT * FROM (SELECT TO_DATE(txn_time) AS txn_day, merchant, amount FROM transactions)
PIVOT (AVG(amount) FOR merchant IN ('Amazon', 'Starbucks', 'Shell'));
