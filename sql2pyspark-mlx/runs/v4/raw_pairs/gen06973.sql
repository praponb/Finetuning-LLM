WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        li_item.qty,
        li_item.price,
        li_item.promo
    FROM carts
    LATERAL VIEW EXPLODE(items) e AS li_item
)
SELECT
    id,
    coupon_codes,
    qty,
    price,
    promo
FROM lines;
