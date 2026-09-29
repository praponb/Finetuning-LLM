SELECT account_id, COUNT(*) row_count, MIN(amount) min_amount FROM transactions GROUP BY account_id HAVING COUNT(*) > 4;
