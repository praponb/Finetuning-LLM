WITH filtered AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        shipping_fee,
        CONCAT_WS('/', status, channel) AS status_channel_key,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM orders
    WHERE order_date >= TO_DATE('2023-12-12')
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee,
        MAX(shipping_fee) OVER (PARTITION BY channel, customer_id) AS channel_max_shipping_fee,
        LAST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY channel, customer_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee_2
    FROM filtered
),
enriched_orders AS (
    SELECT
        *,
        SUM(last_known_shipping_fee) OVER (PARTITION BY channel ORDER BY last_known_shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_shipping_fee,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY shipping_fee DESC, id) AS shipping_fee_bucket,
        LAST_VALUE(shipping_fee) IGNORE NULLS OVER (PARTITION BY channel ORDER BY last_known_shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee_3,
        AVG(last_known_shipping_fee_2) OVER (ORDER BY last_known_shipping_fee, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last_known_shipping_fee_2_avg_3,
        FIRST_VALUE(channel_max_shipping_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY last_known_shipping_fee_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel_max_shipping_fee
    FROM enriched
),
matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM enriched_orders src
    INNER JOIN promotions pr
        ON src.channel = pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date <= DATE_ADD(pr.ends_on, 3)
),
attributed AS (
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM matched a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL 7 DAYS
),
pivoted AS (
    SELECT *
    FROM (
        SELECT
            store_id,
            customer_id,
            promotion_promo_code,
            shipping_fee_bucket
        FROM attributed
    ) src
    PIVOT (
        SUM(shipping_fee_bucket) AS total
        FOR promotion_promo_code IN ('VIP', 'BUNDLE')
    )
)
SELECT
    s.store_id,
    s.customer_id,
    s.VIP,
    (SELECT SUM(a2.balance) FROM accounts a2 WHERE a2.customer_id = s.customer_id) AS sum_account_balance
FROM pivoted s
ORDER BY store_id DESC NULLS LAST, sum_account_balance;
