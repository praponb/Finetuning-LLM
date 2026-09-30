WITH unpacked AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        attributes,
        COALESCE(ship_to.city, 'UNKNOWN') AS city_or_unknown,
        SIZE(items) AS num_items
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
)
SELECT
    id,
    created_at,
    coupon_codes,
    LEAD(num_items) OVER (ORDER BY num_items, id) AS next_num_items,
    ROW_NUMBER() OVER (ORDER BY num_items, id) AS row_seq,
    AVG(num_items) OVER (ORDER BY num_items, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS num_items_avg_5
FROM unpacked;
