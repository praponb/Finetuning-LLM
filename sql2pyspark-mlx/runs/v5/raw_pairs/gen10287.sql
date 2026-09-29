WITH scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        rating,
        MAX(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_rating,
        MIN(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_min_rating,
        COUNT(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_rating
    FROM suppliers
)
SELECT
    id,
    supplier_name,
    rating,
    last3_min_rating,
    CASE WHEN supplier_name RLIKE '^(Pacific Wholesale|Lion City Imports)$' THEN 'GROUP_B' WHEN supplier_name RLIKE '^(Siam Parts Co|Global Supply)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS supplier_name_zone
FROM scored
ORDER BY last3_min_rating DESC
