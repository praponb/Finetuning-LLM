SELECT * FROM (SELECT DATE_FORMAT(paid_at, 'yyyy-MM') AS paid_month, status, amount FROM payments) PIVOT (MAX(amount) FOR status IN ('FAILED', 'SUCCESS')) ORDER BY paid_month
