WITH unpacked AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        items,
        attributes['utm_source'] AS utm_source,
        attributes['gift'] AS gift,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
        AGGREGATE(items, CAST(0 AS DOUBLE), (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS basket_amount,
        ARRAY_CONTAINS(coupon_codes, 'BUNDLE') AS used_bundle
    FROM carts
    WHERE created_at < '2024-06-01'
)
SELECT
    id,
    created_at,
    coupon_codes,
    utm_source,
    basket_amount,
    FIRST_VALUE(weight_kg) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_weight_kg,
    LAST_VALUE(weight_kg) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_weight_kg,
    NTILE(3) OVER w_recent AS basket_amount_bucket,
    AVG(basket_amount) OVER (PARTITION BY customer_id ORDER BY created_at, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS basket_amount_avg_5_prior
FROM unpacked
WINDOW w_recent AS (PARTITION BY customer_id ORDER BY basket_amount, id);
