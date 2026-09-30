WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        created_at,
        items,
        attributes,
        SIZE(items) AS num_items,
        ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price,
        COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
        EXISTS(items, item -> item.price > 100) AS has_premium_item,
        AGGREGATE(items, CAST(0 AS DOUBLE), (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS cart_value
    FROM carts
)
SELECT
    id,
    customer_id,
    num_items,
    max_item_price,
    zip_or_unknown,
    has_premium_item,
    cart_value,
    NTILE(5) OVER (PARTITION BY customer_id ORDER BY cart_value DESC, id ASC) AS cart_value_bucket,
    SUM(cart_value) OVER (ORDER BY max_item_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cart_value
FROM flattened_attrs;
