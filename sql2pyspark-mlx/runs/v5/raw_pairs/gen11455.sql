WITH base AS (
    SELECT
        id,
        email,
        country,
        city,
        segment,
        is_active,
        age,
        CONCAT(country, ' ', city) AS country_city,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM customers
    WHERE signup_date > DATE '2023-07-08'
)
SELECT
    s.id,
    s.country,
    s.city,
    s.segment,
    s.is_active,
    s.country_city,
    s.city_filled,
    top1.session_country,
    top1.device
FROM base s
LEFT JOIN LATERAL (
        SELECT c.country AS session_country, c.device
        FROM sessions c
        WHERE c.customer_id = s.id
        ORDER BY c.started_at DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE
