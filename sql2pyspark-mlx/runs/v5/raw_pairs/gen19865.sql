WITH filtered AS (
    SELECT *, LOWER(city) AS city_lower
    FROM suppliers
    WHERE rating <= 2.6
)
SELECT s.*
FROM filtered s
WHERE EXISTS (
        SELECT 1
        FROM products ch
        WHERE ch.supplier_id = s.id
          AND ch.brand IN ('Initech', 'Acme', 'Stark')
    )
