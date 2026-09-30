WITH filtered AS (
    SELECT *, LENGTH(status) AS status_length
    FROM subscriptions
    WHERE end_date > TO_DATE('2023-05-06')
      AND customer_id IS NOT NULL
)
SELECT status, plan
FROM filtered
INTERSECT
SELECT status, plan
FROM subscriptions
WHERE end_date <= '2025-09-01'
