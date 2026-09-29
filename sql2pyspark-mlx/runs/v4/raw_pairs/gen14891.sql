SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY paid_at, id) AS row_num
    FROM payments
) AS t
WHERE row_num <= 5
ORDER BY paid_at, order_id DESC
