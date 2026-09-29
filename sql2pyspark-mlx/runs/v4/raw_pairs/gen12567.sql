WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT(supplier_name, ', ', country) AS supplier_name_country
    FROM suppliers
    WHERE city IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        country,
        city,
        rating
    FROM cleaned
) src
PIVOT (
    SUM(rating)
    FOR city IN ('New York' AS new_york, 'Berlin' AS berlin, 'London' AS london)
);
