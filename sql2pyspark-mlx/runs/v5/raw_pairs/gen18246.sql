WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date DESC, id) AS rnk
    FROM subscriptions
)
SELECT
    id,
    status,
    start_date,
    monthly_fee
FROM numbered
WHERE rnk <= 5
