WITH cleaned AS (
    SELECT
        *,
        YEAR(opened_date) AS opened_date_year,
        COALESCE(status, 'OTHER') AS status_filled
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-07-09')
      AND status IS NOT NULL
)
SELECT id
FROM cleaned
EXCEPT
SELECT id
FROM accounts
WHERE customer_id = 36;
