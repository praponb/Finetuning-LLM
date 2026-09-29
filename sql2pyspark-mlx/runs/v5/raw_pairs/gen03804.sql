WITH filtered_suppliers AS (
    SELECT *
    FROM suppliers
    WHERE country NOT IN ('JP', 'UK', 'US')
),
agg AS (
    SELECT CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END AS rating_bucket, city, SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM filtered_suppliers
    GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END, city
)
SELECT *
FROM agg
ORDER BY city;
