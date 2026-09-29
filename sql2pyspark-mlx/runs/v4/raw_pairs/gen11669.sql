WITH recent_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE (city <> 'New York' AND LOWER(TRIM(supplier_name)) = 'tokyo traders') OR LENGTH(supplier_name) < 14
),
suppliers_stats AS (
    SELECT country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
    FROM recent_suppliers
    GROUP BY country
)
SELECT *
FROM suppliers_stats
ORDER BY country_values;
