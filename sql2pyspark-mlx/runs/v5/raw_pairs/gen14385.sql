WITH ranked_transactions AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS rnk FROM transactions) SELECT id, merchant, amount FROM ranked_transactions WHERE rnk = 1
