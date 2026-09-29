WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        attributes,
        ship_to.city AS ship_city,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code
    FROM carts
    WHERE customer_id IN (2, 26, 35)
)
-- Aggregate with conditional metrics
SELECT
    ship_city,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(ship_city = 'New York') AS new_york_events
FROM unpacked
GROUP BY ship_city
HAVING COUNT_IF(ship_city = 'New York') > 1;
