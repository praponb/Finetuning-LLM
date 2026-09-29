WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(supplier_name, 1, 3) AS supplier_name_prefix
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Berlin'
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        country,
        rating
    FROM base
) src
PIVOT (
    AVG(rating)
    FOR country IN ('SG', 'TH')
);
