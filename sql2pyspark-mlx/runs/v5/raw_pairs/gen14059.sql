SELECT id, merchant, amount, RANK() OVER (PARTITION BY merchant ORDER BY amount) AS rnk FROM transactions ORDER BY merchant, amount;
