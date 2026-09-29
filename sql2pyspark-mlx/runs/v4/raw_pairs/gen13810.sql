WITH filtered AS (
    SELECT
        *,
        INITCAP(TRIM(segment)) AS segment_title,
        LOWER(email) AS email_lower,
        COALESCE(city, 'N/A') AS resolved_city
    FROM customers
    WHERE signup_date > TO_DATE('2023-05-20')
)
SELECT *, RANK() OVER (PARTITION BY segment ORDER BY credit_limit DESC) AS row_num
FROM filtered
QUALIFY row_num = 1;
