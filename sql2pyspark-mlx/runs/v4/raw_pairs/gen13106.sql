WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        WEEKOFYEAR(started_at) started_at_weekofyear,
        SUBSTRING(browser, 1, 2) browser_prefix
    FROM sessions
    WHERE started_at >= TO_TIMESTAMP('2023-09-29 01:00:00')
      AND customer_id IS NOT NULL
)
SELECT id
FROM filtered
EXCEPT
SELECT id
FROM sessions
WHERE customer_id IN (13, 18, 23, 24)
