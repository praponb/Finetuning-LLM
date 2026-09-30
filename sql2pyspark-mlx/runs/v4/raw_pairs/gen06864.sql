SELECT id, status, paid_at
FROM (
    SELECT
        id,
        status,
        paid_at,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS row_num
    FROM payments
) AS r
WHERE row_num <= 1
ORDER BY id DESC, status ASC
