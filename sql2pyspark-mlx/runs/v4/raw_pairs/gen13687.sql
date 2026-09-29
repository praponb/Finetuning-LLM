SELECT
    id,
    items,
    ship_to,
    TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
    COALESCE(ship_to.city, 'UNKNOWN') AS city_or_unknown,
    ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10
FROM carts;
