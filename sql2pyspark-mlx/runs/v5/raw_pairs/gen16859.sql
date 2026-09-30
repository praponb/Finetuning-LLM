WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        PERCENT_RANK() OVER (PARTITION BY channel, store_id ORDER BY amount DESC) AS amount_percent_rank,
        LAST_VALUE(channel, TRUE) OVER w_ordered AS last_known_channel_2,
        LAST_VALUE(amount, TRUE) OVER w_recent AS last_known_amount,
        RANK() OVER (ORDER BY discount DESC) AS discount_rank
    FROM orders
    WINDOW w_ordered AS (PARTITION BY channel, customer_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY store_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.customer_id,
    s.order_date,
    s.amount,
    s.discount,
    s.shipping_fee,
    s.last_known_channel,
    s.amount_percent_rank,
    s.last_known_channel_2,
    (SELECT MAX(t2.monthly_fee) FROM subscriptions t2 WHERE t2.customer_id = s.customer_id) AS max_subscription_monthly_fee
FROM scored s
ORDER BY discount DESC NULLS FIRST, order_date;
