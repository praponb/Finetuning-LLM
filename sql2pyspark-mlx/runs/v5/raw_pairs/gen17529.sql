WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        items,
        ship_to,
        ship_to.country AS ship_country,
        attributes['gift'] AS gift,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        COALESCE(attributes['device'], 'none') AS device_or_default,
        SIZE(attributes) AS num_attributes
    FROM carts
)
-- Aggregate with conditional metrics
SELECT
    ship_country,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT device_or_default) AS distinct_device_or_defaults,
    ROUND(SUM(num_attributes) / NULLIF(COUNT(CASE WHEN ship_country = 'UK' THEN 1 END), 0), 2) AS num_attributes_per_uk,
    COUNT(*) FILTER (WHERE ship_country = 'UK') AS uk_rows
FROM unpacked
GROUP BY ship_country
HAVING num_rows > 1
ORDER BY distinct_device_or_defaults;
