SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS rn FROM payments) AS t WHERE rn <= 3
