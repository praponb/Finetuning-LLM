WITH filtered AS (
    SELECT *, UPPER(status) status_upper
    FROM orders
    WHERE order_date > DATE '2023-02-25'
      AND store_id IN (13, 31, 39)
)
SELECT
    id,
    order_date,
    amount,
    shipping_fee,
    AVG(shipping_fee) OVER w_running shipping_fee_avg_6,
    FIRST_VALUE(channel, TRUE) OVER (ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_channel,
    LAST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_shipping_fee
FROM filtered
WHERE status != 'CANCELLED'
WINDOW w_running AS (PARTITION BY channel, status ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);
