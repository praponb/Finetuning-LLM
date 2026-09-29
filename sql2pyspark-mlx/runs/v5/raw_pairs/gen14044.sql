SELECT MONTH(paid_at) paid_month, YEAR(paid_at) paid_year, ROUND(AVG(amount), 2) avg_amount FROM payments GROUP BY MONTH(paid_at), YEAR(paid_at);
