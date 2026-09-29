WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(TRIM(email)) AS email_normalized,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM customers
    WHERE segment IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        country,
        segment,
        credit_limit
    FROM cleaned
) src
PIVOT (
    AVG(credit_limit) AS total,
    COUNT(1) AS n
    FOR segment IN ('SMB', 'Consumer', 'Corporate')
);
