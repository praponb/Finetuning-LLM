WITH filtered AS (
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM orders
    WHERE order_date < '2025-01-01'
)
SELECT
    *,
    CORR(shipping_fee, discount) OVER (PARTITION BY customer_id) AS corr_shipping_fee_discount,
    ROW_NUMBER() OVER (ORDER BY amount, id) AS row_seq,
    LAG(shipping_fee, 1) OVER (PARTITION BY customer_id ORDER BY order_date, id) AS prev_shipping_fee,
    NTILE(5) OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket,
    NTILE(3) OVER (PARTITION BY channel ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket_2
FROM filtered
