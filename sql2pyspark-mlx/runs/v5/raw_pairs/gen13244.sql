SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount DESC, id) AS row_num
    FROM transactions
) AS t
WHERE row_num <= 3;
