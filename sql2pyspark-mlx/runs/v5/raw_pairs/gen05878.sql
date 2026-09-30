WITH flattened_attrs AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10
    FROM carts
)
SELECT
    *,
    NTILE(10) OVER (ORDER BY weight_kg DESC, id) AS weight_kg_bucket,
    LEAD(weight_kg, 1) OVER (ORDER BY created_at, id) AS next_weight_kg
FROM flattened_attrs
WHERE TO_DATE(created_at) < '2023-06-27';
