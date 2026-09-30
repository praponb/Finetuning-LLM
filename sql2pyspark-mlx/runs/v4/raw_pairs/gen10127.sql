SELECT id, segment, credit_limit
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS rn
    FROM customers
) AS ranked
WHERE rn <= 5
ORDER BY id DESC;
