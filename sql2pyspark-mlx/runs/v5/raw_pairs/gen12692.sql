WITH valid AS (
    SELECT
        *,
        LOWER(status) status_lower,
        INITCAP(TRIM(account_type)) account_type_title
    FROM accounts
    WHERE opened_date >= DATE '2023-07-18'
      AND status IS DISTINCT FROM 'CLOSED'
)
SELECT *
FROM (
    SELECT
        account_type,
        status,
        balance
    FROM valid
) src
PIVOT (
    AVG(balance) AS amount,
    COUNT(1) AS n
    FOR status IN ('CLOSED', 'OPEN')
);
