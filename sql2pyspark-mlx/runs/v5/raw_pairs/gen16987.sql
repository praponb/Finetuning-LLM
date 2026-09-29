SELECT TO_DATE(paid_at) AS paid_day, MAX(paid_at) AS latest_paid_at FROM payments WHERE HOUR(paid_at) BETWEEN 7 AND 10 AND amount IS NOT NULL GROUP BY TO_DATE(paid_at) ORDER BY paid_day
