WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS row_seq_2,
        LEAD(rating, 1, 0) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating,
        AVG(rating) OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS rating_avg_3
    FROM suppliers
)
-- Multi-level aggregation
SELECT
    country,
    COUNT_IF(city = 'Berlin') AS berlin_events,
    COUNT(*) FILTER (WHERE city = 'New York') AS new_york_rows,
    MIN(rating_avg_3) AS min_rating_avg_3,
    SUM(CASE WHEN city = 'Bangkok' THEN rating ELSE 0 END) AS bangkok_rating,
    COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows
FROM with_windows
GROUP BY country
HAVING new_york_rows > 1
ORDER BY berlin_events DESC NULLS FIRST
