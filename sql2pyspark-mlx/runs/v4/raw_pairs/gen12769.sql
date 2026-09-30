WITH base AS (
    SELECT *, amount - shipping_fee AS amount_with_shipping
    FROM orders
    WHERE channel NOT IN ('partner', 'mobile', 'web')
)
SELECT
    *,
    NTILE(10) OVER (PARTITION BY customer_id ORDER BY amount_with_shipping DESC, id) AS amount_with_shipping_bucket,
    AVG(discount) OVER (PARTITION BY channel ORDER BY amount_with_shipping, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS discount_avg_2_prior
FROM base;
