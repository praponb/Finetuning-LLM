WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE city IN ('Chiang Mai', 'Berlin') AND city IS NOT NULL
),
agg AS (
    SELECT country, AVG(rating) AS avg_rating
    FROM filtered_suppliers
    GROUP BY country
)
SELECT *
FROM agg
WHERE avg_rating >= 3.9
ORDER BY 1;
