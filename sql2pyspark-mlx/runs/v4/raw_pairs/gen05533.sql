WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        attributes,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'device') AS has_device,
        attributes['utm_source'] AS utm_source,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg
    FROM carts
)
-- Ranking and gap features
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY weight_kg DESC, id) AS seq_no,
    SUM(weight_kg) OVER w_hist AS weight_kg_sum_30m
FROM flattened_attrs
WINDOW w_hist AS (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW)
