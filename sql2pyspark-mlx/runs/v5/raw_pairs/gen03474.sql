WITH numbered AS (SELECT id, account_id, amount, merchant, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount, id) AS pos FROM transactions) SELECT * FROM numbered WHERE pos <= 3;
