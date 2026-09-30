SELECT CASE WHEN status IN ('REFUNDED', 'SUCCESS') THEN 'priority' ELSE 'other' END AS status_group, MAX(amount) AS max_amount, SUM(amount) AS total_amount FROM payments GROUP BY 1
