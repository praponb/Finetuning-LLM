WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount, id) AS rn FROM transactions) SELECT id, merchant, amount FROM numbered WHERE rn <= 3 ORDER BY amount DESC;
