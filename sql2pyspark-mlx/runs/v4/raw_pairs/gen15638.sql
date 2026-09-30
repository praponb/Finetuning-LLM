SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY paid_at ASC NULLS LAST, id ASC) AS rn
    FROM payments
) x
WHERE rn = 1;
