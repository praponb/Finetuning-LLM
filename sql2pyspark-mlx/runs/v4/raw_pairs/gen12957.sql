WITH cleaned AS (
    SELECT *, LOWER(status) AS status_lower
    FROM subscriptions
    WHERE monthly_fee BETWEEN 80 AND 160
),
valid AS (
    SELECT
        id,
        plan,
        start_date,
        monthly_fee,
        status,
        DATE_TRUNC('week', end_date) AS end_date_period
    FROM cleaned
    WHERE end_date >= DATE '2023-10-26'
      AND monthly_fee <= 70
),
ranked AS (
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY start_date DESC, id) = 1
),
kept_keys AS (
    SELECT status, id
    FROM ranked
    INTERSECT
    SELECT status, id
    FROM subscriptions
    WHERE status = 'PAUSED'
),
reconciled AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.status = k.status AND s.id = k.id
)
SELECT
    start_date,
    status,
    end_date_period
FROM reconciled;
