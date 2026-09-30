SELECT
    id,
    status,
    start_date,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY start_date, id) AS rn
FROM subscriptions
WHERE plan != 'premium'
ORDER BY rn;
