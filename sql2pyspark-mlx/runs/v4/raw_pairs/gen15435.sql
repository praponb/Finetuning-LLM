WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), started_at) AS days_since_started_at
    FROM sessions
    WHERE browser = 'Chrome'
)
SELECT country, id
FROM filtered
EXCEPT DISTINCT
SELECT country, id
FROM sessions
WHERE customer_id = 13;
