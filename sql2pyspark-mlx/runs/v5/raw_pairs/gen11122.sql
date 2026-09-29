WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        TRANSFORM(items, i -> i.product_id) AS product_ids,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        SIZE(attributes) AS num_attributes,
        SIZE(items) AS num_items
    FROM carts
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    NTILE(10) OVER (ORDER BY num_items DESC, id) AS num_items_bucket,
    LEAD(num_items, 1) OVER (PARTITION BY customer_id ORDER BY num_attributes, id) AS next_num_items,
    LAG(num_items, 1) OVER (PARTITION BY customer_id ORDER BY created_at, id) AS prev_num_items,
    COUNT(num_attributes) OVER w_part AS customer_id_count_num_attributes,
    NTILE(10) OVER w_running AS num_items_bucket_2
FROM parsed
WINDOW w_part AS (PARTITION BY customer_id), w_running AS (PARTITION BY customer_id ORDER BY num_items, id)
