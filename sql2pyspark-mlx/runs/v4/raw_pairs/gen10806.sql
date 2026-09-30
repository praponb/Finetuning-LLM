WITH filtered AS (
    SELECT
        id,
        country,
        city,
        rating,
        COALESCE(rating, 0) AS rating_filled,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city = 'Singapore'
),
valid AS (
    SELECT
        id,
        country,
        city,
        CONCAT(city, ' ', country) AS city_country,
        COALESCE(city, 'OTHER') AS city_filled_2
    FROM filtered
    WHERE country = 'DE'
),
latest_match AS (
    SELECT s.*, top1.price
    FROM valid s
    JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    country,
    city_country,
    city_filled_2,
    price
FROM latest_match
ORDER BY country DESC NULLS LAST
