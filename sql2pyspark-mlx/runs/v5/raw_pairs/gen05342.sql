WITH Ranked AS (
    SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY amount DESC) pos
    FROM transactions
)
SELECT *
FROM Ranked
WHERE pos <= 3
ORDER BY merchant DESC;
