SELECT id, amount, txn_time, merchant FROM transactions WHERE amount >= (SELECT MAX(amount) FROM transactions) ORDER BY amount
