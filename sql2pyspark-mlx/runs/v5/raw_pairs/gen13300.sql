SELECT
    id,
    plan,
    status
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY plan ORDER BY start_date, id) AS dedup_rank
    FROM subscriptions
) ranked
WHERE dedup_rank = 1;
