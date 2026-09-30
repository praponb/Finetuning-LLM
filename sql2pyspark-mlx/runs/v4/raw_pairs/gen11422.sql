WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        ship_to,
        SIZE(attributes) AS num_attributes,
        ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price,
        FORALL(items, x -> x.qty <= 5) AS all_small_qty,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10,
        COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default
    FROM carts
)
-- Window metrics over several different partitions
SELECT
    *,
    FIRST_VALUE(max_item_price) IGNORE NULLS OVER (ORDER BY max_item_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_max_item_price,
    AVG(max_item_price) OVER (ORDER BY num_attributes, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS max_item_price_avg_6,
    ROW_NUMBER() OVER (ORDER BY max_item_price DESC, id) AS event_seq,
    LEAD(max_item_price, 1) OVER (ORDER BY num_attributes, id) AS next_max_item_price,
    RANK() OVER (ORDER BY max_item_price DESC NULLS FIRST) AS max_item_price_rank
FROM flattened_attrs
