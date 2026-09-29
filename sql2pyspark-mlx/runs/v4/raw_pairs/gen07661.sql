SELECT
    *,
    FIRST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY customer_id, store_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_shipping_fee,
    FIRST_VALUE(discount) IGNORE NULLS OVER (PARTITION BY channel, customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount,
    FIRST_VALUE(discount, TRUE) OVER (PARTITION BY store_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_2,
    AVG(amount) OVER (PARTITION BY store_id, status ORDER BY shipping_fee, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
    ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount, id) AS event_seq
FROM orders
