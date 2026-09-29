SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY amount DESC, id) AS rn
    FROM payments
) AS t
WHERE rn <= 1
ORDER BY paid_at ASC;
