SELECT id, txn_time, amount, ROW_NUMBER() OVER (ORDER BY txn_time, id) AS row_num, LEAD(amount, 2) OVER (ORDER BY txn_time, id) AS next_amount FROM transactions ORDER BY next_amount DESC;
