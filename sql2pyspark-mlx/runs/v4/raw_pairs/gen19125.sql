SELECT * FROM (SELECT TO_DATE(paid_at) AS paid_day, status, amount FROM payments) PIVOT (SUM(amount) FOR status IN ('REFUNDED', 'FAILED'));
