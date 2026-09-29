WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        items,
        ship_to.city AS ship_city,
        attributes['device'] AS device,
        SIZE(items) AS num_items,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg
    FROM carts
)
SELECT
    *,
    NTILE(3) OVER (ORDER BY num_items, id) AS num_items_bucket,
    SUM(num_items) OVER (PARTITION BY customer_id ORDER BY weight_kg, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_num_items,
    AVG(num_items) OVER (ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS num_items_avg_1h
FROM flattened_attrs
