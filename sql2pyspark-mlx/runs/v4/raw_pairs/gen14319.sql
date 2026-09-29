WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        coupon_codes,
        items,
        ship_to,
        COALESCE(ship_to.country, 'UNKNOWN') AS country_or_unknown,
        AGGREGATE(items, 0D, (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS items_total,
        ship_to.city AS ship_city
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
)
-- Running and moving aggregates
SELECT
    id,
    customer_id,
    coupon_codes,
    items,
    ship_to,
    country_or_unknown,
    items_total,
    ship_city,
    NTILE(10) OVER w_grp AS items_total_bucket,
    AVG(items_total) OVER (PARTITION BY ship_city, customer_id ORDER BY items_total ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS items_total_avg_3,
    ROW_NUMBER() OVER (ORDER BY items_total ASC, id ASC) AS row_seq
FROM unpacked
WINDOW w_grp AS (PARTITION BY customer_id ORDER BY items_total DESC, id ASC)
