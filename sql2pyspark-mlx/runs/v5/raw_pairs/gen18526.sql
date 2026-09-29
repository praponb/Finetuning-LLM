WITH FilteredSuppliers AS (
    SELECT *, COALESCE(supplier_name, 'N/A') AS supplier_name_filled
    FROM suppliers
    WHERE rating > 2.1
),
agg AS (
    SELECT CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END AS rating_bucket, country, MIN(rating) AS min_rating
    FROM FilteredSuppliers
    GROUP BY CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 2.2 THEN 'mid' ELSE 'low' END, country
)
SELECT *
FROM agg;
