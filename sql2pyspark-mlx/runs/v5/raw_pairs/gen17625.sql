WITH src AS (
    SELECT *
    FROM suppliers
    WHERE NOT (rating < 2.1 AND LENGTH(supplier_name) > 5)
),
summary AS (
    SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
    FROM src
    GROUP BY city
)
SELECT city, country_values
FROM summary
