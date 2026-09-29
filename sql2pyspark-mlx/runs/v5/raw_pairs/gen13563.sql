WITH filtered AS (
    SELECT
        *,
        UPPER(status) AS status_upper,
        COALESCE(status, 'N/A') AS status_filled
    FROM accounts
    WHERE account_type IN ('CHECKING', 'CREDIT')
      AND opened_date >= DATE '2023-06-12'
)
SELECT id, status
FROM filtered
INTERSECT ALL
SELECT id, status
FROM accounts
WHERE status IS NOT NULL;
