WITH cleaned AS (
    SELECT *, ROUND(rating, 1) AS rating_rounded
    FROM suppliers
    WHERE city IS NOT NULL
      AND UPPER(TRIM(supplier_name)) = 'TOKYO TRADERS'
),
not_flagged AS (
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.stock_qty BETWEEN 137 AND 224
        )
),
remaining_keys AS (
    SELECT city
    FROM not_flagged
    INTERSECT
    SELECT city
    FROM suppliers
    WHERE city = 'London'
),
verified AS (
    SELECT s.*
    FROM not_flagged s
    INNER JOIN remaining_keys k ON s.city = k.city
),
cleaned_suppliers AS (
    SELECT
        id,
        supplier_name,
        rating,
        rating_rounded,
        CONCAT(country, ' - ', supplier_name) AS country_supplier_name,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM verified
    WHERE supplier_name LIKE 'Gl%'
),
cleaned_suppliers_2 AS (
    SELECT
        id,
        supplier_name,
        rating,
        rating_rounded,
        country_supplier_name,
        resolved_city,
        rating_rounded + rating AS rating_rounded_plus_rating,
        ROUND(rating_rounded * 100, 2) AS adjusted_rating_rounded
    FROM cleaned_suppliers
    WHERE supplier_name LIKE '%Parts%'
)
SELECT
    id,
    rating,
    rating_rounded,
    rating_rounded_plus_rating,
    adjusted_rating_rounded
FROM cleaned_suppliers_2
ORDER BY adjusted_rating_rounded DESC;
