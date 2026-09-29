WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        pos + 1 AS item_no,
        item.product_id,
        item.qty,
        item.promo
    FROM carts
    LATERAL VIEW OUTER POSEXPLODE(items) e AS pos, item
)
SELECT
    item_no,
    product_id,
    qty,
    promo
FROM lines
ORDER BY item_no DESC, product_id DESC
