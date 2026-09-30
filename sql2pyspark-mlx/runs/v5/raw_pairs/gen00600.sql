WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        account_type,
        opened_date,
        status
    FROM accounts
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC, id) = 1
)
SELECT
    id,
    opened_date,
    status,
    CASE WHEN status RLIKE '^(CLOSED|OPEN)$' THEN 'EXTENDED' WHEN status RLIKE '^(FROZEN)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS status_zone
FROM ranked
