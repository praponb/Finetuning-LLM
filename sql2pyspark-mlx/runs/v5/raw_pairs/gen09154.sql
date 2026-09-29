WITH valid AS (
    SELECT *, LOWER(supplier_name) supplier_name_lower
    FROM suppliers
    WHERE city IS NOT NULL
      AND supplier_name IS NOT NULL
)
SELECT *
FROM (
    SELECT
        country,
        city,
        rating
    FROM valid
) src
PIVOT (
    AVG(rating)
    FOR city IN ('Bangkok', 'London')
);
