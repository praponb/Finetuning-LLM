SELECT id, txn_type, amount FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount DESC, id) AS rn FROM transactions) AS t WHERE rn <= 5
