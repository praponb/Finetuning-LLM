WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        INITCAP(TRIM(city)) AS city_title
    FROM suppliers
    WHERE city IS NOT DISTINCT FROM 'New York'
      AND city IS NOT NULL
)
-- Running and moving aggregates
SELECT
    id,
    supplier_name,
    AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_rating,
    MAX(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_rating,
    SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_sum_rating
FROM valid;
