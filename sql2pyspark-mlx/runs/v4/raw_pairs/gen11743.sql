WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        ship_to,
        ship_to.zip AS ship_zip,
        ship_to.country AS ship_country,
        COALESCE(attributes['device'], 'none') AS device_or_default,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        TRANSFORM(items, item -> item.product_id) AS product_ids
    FROM carts
    WHERE customer_id IS NULL
)
-- Grouped metrics
SELECT
    ship_country,
    COUNT_IF(ship_country = 'DE') AS de_events,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT_IF(ship_country = 'SG') AS sg_events,
    COUNT(*) FILTER (WHERE ship_country = 'TH') AS th_rows
FROM flattened_attrs
GROUP BY 1
HAVING COUNT(DISTINCT id) >= 1
ORDER BY th_rows DESC;
