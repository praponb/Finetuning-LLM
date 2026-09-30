WITH valid AS (
    SELECT *, CASE WHEN country IN ('UK', 'SG') THEN 'Group A' ELSE 'Other' END AS country_group
    FROM suppliers
    WHERE city IS NOT NULL
)
SELECT s.*
FROM valid s
WHERE NOT EXISTS (
        SELECT 1
        FROM products x
        WHERE x.supplier_id = s.id
          AND x.supplier_id = 17
    );
