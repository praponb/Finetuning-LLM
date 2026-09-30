SELECT
    id,
    plan,
    start_date,
    end_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date, id) AS pos
    FROM subscriptions
) AS ranked
WHERE pos <= 2
