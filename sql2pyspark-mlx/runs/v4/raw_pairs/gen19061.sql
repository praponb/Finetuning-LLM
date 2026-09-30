WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        created_at,
        items,
        attributes,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
        TRANSFORM(
            FILTER(items, i -> i.promo IS NOT NULL),
            i -> STRUCT(i.product_id AS pid, ROUND(i.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        TRANSFORM(items, i -> i.product_id) AS product_ids
    FROM carts
)
-- Window metrics over several different partitions
SELECT
    *,
    LEAD(weight_kg, 1, 0) OVER (ORDER BY weight_kg, id) AS next_weight_kg,
    NTILE(3) OVER (PARTITION BY customer_id ORDER BY weight_kg DESC, id) AS weight_kg_bucket
FROM unpacked;
