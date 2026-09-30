SELECT id, merchant FROM transactions WHERE amount >= 1100 UNION SELECT id, merchant FROM transactions WHERE amount BETWEEN 1600 AND 2100
