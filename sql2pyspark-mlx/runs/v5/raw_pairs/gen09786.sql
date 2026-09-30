WITH deduped AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status
    FROM (
        SELECT *, RANK() OVER (PARTITION BY method ORDER BY paid_at DESC) AS latest_rank
        FROM payments
    ) x
    WHERE latest_rank = 1
)
SELECT id, amount
FROM deduped
ORDER BY amount, id DESC;
