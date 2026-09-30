WITH Numbered AS (
    SELECT
        id,
        status,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS rn
    FROM payments
)
SELECT id, status, paid_at
FROM Numbered
WHERE rn <= 3
ORDER BY status;
