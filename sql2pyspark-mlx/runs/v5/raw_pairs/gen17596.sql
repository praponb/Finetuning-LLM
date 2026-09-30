WITH valid AS (
    SELECT *, ROUND(shipping_fee * 0.9, 2) AS shipping_fee_scaled
    FROM orders
    WHERE order_date >= DATE '2023-08-11'
      AND channel IS NOT NULL
)
SELECT
    id,
    store_id,
    order_date,
    status,
    channel,
    discount,
    shipping_fee,
    shipping_fee_scaled,
    CORR(discount, shipping_fee) OVER (PARTITION BY store_id) AS corr_discount_shipping_fee,
    NTILE(5) OVER (PARTITION BY customer_id ORDER BY shipping_fee_scaled, id) AS shipping_fee_scaled_bucket,
    LAST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount
FROM valid
WHERE status = 'SHIPPED';
