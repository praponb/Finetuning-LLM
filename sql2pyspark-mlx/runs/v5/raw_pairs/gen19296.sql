WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        line.product_id,
        line.qty,
        line.price
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            coupon_codes,
            EXPLODE(items) AS line
        FROM carts
    ) li
)
SELECT
    customer_id,
    created_at,
    price,
    SUM(product_id) OVER (PARTITION BY customer_id) AS customer_id_sum_product_id,
    PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY price DESC NULLS FIRST) AS price_percent_rank
FROM lines;
