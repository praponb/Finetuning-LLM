WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        customer_id,
        ship_to,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
        AGGREGATE(items, 0.0D, (acc, i) -> acc + i.qty * i.price, acc -> ROUND(acc, 2)) AS basket_amount,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        COALESCE(attributes['device'], 'direct') AS device_or_default
    FROM carts
)
-- Ranking and gap features
SELECT
    id,
    customer_id,
    utm_source_or_default,
    basket_amount,
    LAG(basket_amount, 2, 0) OVER (PARTITION BY customer_id ORDER BY basket_amount, id) prev_basket_amount,
    CUME_DIST() OVER (PARTITION BY customer_id ORDER BY basket_amount) basket_amount_cume_dist,
    SUM(basket_amount) OVER (ORDER BY basket_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_basket_amount,
    SUM(basket_amount) OVER (ORDER BY basket_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_basket_amount_2
FROM parsed
