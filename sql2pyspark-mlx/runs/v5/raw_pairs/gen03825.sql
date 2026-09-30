WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        coupon_codes,
        ship_to,
        attributes,
        SIZE(items) AS num_items,
        ship_to.country AS ship_country,
        attributes['device'] AS device,
        attributes['gift'] AS gift
    FROM carts
)
-- Ranking and gap features
SELECT
    *,
    NTILE(4) OVER (ORDER BY num_items DESC, id) AS num_items_bucket,
    FIRST_VALUE(device, TRUE) OVER w_part AS first_known_device,
    DENSE_RANK() OVER (PARTITION BY ship_country ORDER BY num_items NULLS LAST) AS num_items_dense_rank
FROM parsed
WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
WINDOW w_part AS (PARTITION BY ship_country ORDER BY num_items, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
