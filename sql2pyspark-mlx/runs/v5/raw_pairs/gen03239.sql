SELECT
    id,
    customer_id,
    started_at,
    browser
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at DESC, id) AS rnk
    FROM sessions
) AS r
WHERE rnk = 1;
