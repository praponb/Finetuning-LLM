SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY paid_at) AS rn
    FROM payments
) ranked
WHERE rn = 1;
