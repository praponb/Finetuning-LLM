WITH ranked_transactions AS (SELECT *, DENSE_RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) rnk FROM transactions) SELECT * FROM ranked_transactions WHERE rnk <= 2 ORDER BY txn_time DESC
