WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        coupon_codes,
        items,
        ship_to,
        AGGREGATE(items, 0.0D, (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS items_total,
        COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default,
        FORALL(items, item -> item.qty <= 5) AS all_small_qty
    FROM carts
)
-- Running and moving aggregates
SELECT
    *,
    DENSE_RANK() OVER (ORDER BY items_total NULLS FIRST) AS items_total_dense_rank,
    AVG(items_total) OVER (ORDER BY items_total, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS items_total_avg_3_prior
FROM flattened_attrs;
