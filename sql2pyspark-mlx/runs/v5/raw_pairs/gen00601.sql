WITH base AS (
    SELECT
        *,
        CASE WHEN status = 'PAUSED' THEN 1 ELSE 0 END AS is_paused,
        ADD_MONTHS(start_date, 1) AS start_date_plus_months
    FROM subscriptions
    WHERE end_date > DATE '2023-10-09'
      AND plan = 'premium'
)
SELECT id, status
FROM base
INTERSECT
SELECT id, status
FROM subscriptions
WHERE end_date > '2023-08-09'
