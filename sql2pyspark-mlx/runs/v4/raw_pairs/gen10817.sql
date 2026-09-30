WITH src AS (
    SELECT *
    FROM accounts
    WHERE status = 'OPEN'
),
agg AS (
    SELECT status, COUNT(DISTINCT balance) unique_account_types, SORT_ARRAY(COLLECT_SET(status)) status_list
    FROM src
    GROUP BY status
)
SELECT status, unique_account_types, status_list
FROM agg
ORDER BY unique_account_types DESC, status
LIMIT 10;
