SELECT *
FROM (
    SELECT
        id,
        method,
        paid_at,
        amount,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) rn
    FROM payments
    WHERE method <> 'bank_transfer'
) r
WHERE rn <= 1
ORDER BY paid_at
