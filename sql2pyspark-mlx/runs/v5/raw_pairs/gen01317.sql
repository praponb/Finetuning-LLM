SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY amount DESC, id) AS rnk
    FROM transactions
) AS t
WHERE rnk = 1
ORDER BY 4 ASC, id ASC;
