WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        attributes,
        COALESCE(attributes['device'], 'unknown') AS device_or_default,
        ship_to.country AS ship_country
    FROM carts
)
SELECT
    id,
    customer_id,
    device_or_default,
    ship_country,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC, id) AS event_seq
FROM flattened_attrs;
