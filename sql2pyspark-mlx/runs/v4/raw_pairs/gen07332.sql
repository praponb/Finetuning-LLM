SELECT YEAR(paid_at) paid_year, COUNT(amount) amount_count FROM payments GROUP BY YEAR(paid_at) ORDER BY amount_count DESC
