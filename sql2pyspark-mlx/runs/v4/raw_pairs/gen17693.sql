WITH filtered AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CONCAT(country, ' - ', city) AS country_city
    FROM suppliers
    WHERE city IS NOT NULL
      AND rating > 3.1
)
SELECT
    *,
    LAG(rating, 1) OVER (PARTITION BY city ORDER BY rating, id) AS prev_rating,
    ROW_NUMBER() OVER (ORDER BY rating, id) AS seq_no
FROM filtered
