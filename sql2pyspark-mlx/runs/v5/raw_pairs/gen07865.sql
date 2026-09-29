WITH src AS (
    SELECT *
    FROM store_sales
    WHERE units BETWEEN 27 AND 40
),
agg AS (
    SELECT store_id, MIN(units) min_units
    FROM src
    GROUP BY store_id
)
SELECT store_id, min_units
FROM agg
WHERE min_units < 36
