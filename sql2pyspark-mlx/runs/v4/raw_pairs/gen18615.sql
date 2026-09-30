WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        ship_to,
        attributes,
        EXISTS(items, x -> x.price > 100) AS has_premium_item,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        FORALL(items, x -> x.qty <= 5) AS all_small_qty
    FROM carts
    WHERE customer_id IN (15, 34, 39)
)
-- Window metrics over several different partitions
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY weight_kg, id) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY weight_kg, id) AS event_seq,
    NTILE(4) OVER (ORDER BY weight_kg DESC, id) AS weight_kg_bucket
FROM flattened_attrs
