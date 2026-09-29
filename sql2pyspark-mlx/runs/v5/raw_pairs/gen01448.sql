WITH flattened_attrs AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        ship_to,
        attributes,
        TRANSFORM(items, i -> i.product_id) AS product_ids,
        attributes['utm_source'] AS utm_source
    FROM carts
    WHERE created_at < '2024-09-05'
)
SELECT
    *,
    FIRST_VALUE(utm_source) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY created_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_utm_source,
    ROW_NUMBER() OVER w_grp seq_no,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at, id) row_seq
FROM flattened_attrs
WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
WINDOW w_grp AS (PARTITION BY customer_id ORDER BY created_at DESC, id)
