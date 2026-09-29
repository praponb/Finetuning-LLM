WITH Ranked AS (
    SELECT
        id,
        status,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS rn
    FROM payments
    WHERE HOUR(paid_at) BETWEEN 8 AND 14
)
SELECT *
FROM Ranked
WHERE rn = 1
ORDER BY 1 DESC
