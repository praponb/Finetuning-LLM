SELECT id, amount, txn_time, is_flagged FROM transactions WHERE amount > (SELECT AVG(amount) FROM transactions) ORDER BY amount;
