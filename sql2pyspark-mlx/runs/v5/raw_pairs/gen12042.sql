WITH numbered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS rnk FROM payments) SELECT id, method, amount, status FROM numbered WHERE rnk = 1
