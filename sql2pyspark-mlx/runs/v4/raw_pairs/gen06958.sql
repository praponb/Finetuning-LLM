WITH Numbered AS (
    SELECT
        id,
        status,
        end_date,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY end_date DESC, id) AS rnk
    FROM subscriptions
)
SELECT
    id,
    status,
    end_date,
    start_date
FROM Numbered
WHERE rnk = 1
ORDER BY start_date DESC;
