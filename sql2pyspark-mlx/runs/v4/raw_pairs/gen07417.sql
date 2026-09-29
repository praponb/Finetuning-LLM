WITH valid_payments AS (
    SELECT *
    FROM payments
    WHERE order_id IN (1, 15, 25) AND order_id IS NOT NULL AND amount BETWEEN 840 AND 1080
),
Summary AS (
    SELECT status, MAX(paid_at) AS latest_paid_at
    FROM valid_payments
    GROUP BY status
)
SELECT status, latest_paid_at
FROM Summary
