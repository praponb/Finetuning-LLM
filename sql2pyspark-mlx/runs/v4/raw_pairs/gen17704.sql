WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END AS is_success
    FROM payments
    WHERE paid_at >= TO_DATE('2023-07-01')
      AND order_id IS NOT NULL
      AND method = 'bank_transfer'
)
SELECT order_id
FROM base
EXCEPT
SELECT order_id
FROM payments
WHERE amount < 700;
