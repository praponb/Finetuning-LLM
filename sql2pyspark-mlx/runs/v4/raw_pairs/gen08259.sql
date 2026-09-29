WITH src AS (
    SELECT *
    FROM suppliers
    WHERE city <> 'Singapore'
),
agg AS (
    SELECT city, SORT_ARRAY(COLLECT_SET(country)) country_list
    FROM src
    GROUP BY city
)
SELECT *
FROM agg
ORDER BY city
LIMIT 20;
