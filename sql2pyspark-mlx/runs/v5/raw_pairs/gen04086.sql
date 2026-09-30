WITH base AS (
    SELECT *
    FROM suppliers
    WHERE UPPER(TRIM(supplier_name)) = 'SIAM PARTS CO' AND city = 'Singapore'
),
summary AS (
    SELECT city, SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END) AS london_count, SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM base
    GROUP BY city
)
SELECT *
FROM summary
WHERE london_count > 27
ORDER BY city, london_count DESC;
