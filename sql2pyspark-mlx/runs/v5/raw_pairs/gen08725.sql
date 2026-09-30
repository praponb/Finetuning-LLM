WITH filtered AS (
    SELECT *, LAST_DAY(opened_date) AS opened_date_month_end
    FROM accounts
    WHERE opened_date > TO_DATE('2023-06-03')
      AND status IS DISTINCT FROM 'FROZEN'
)
SELECT customer_id, account_type
FROM filtered
INTERSECT ALL
SELECT customer_id, account_type
FROM accounts
WHERE status = 'OPEN';
