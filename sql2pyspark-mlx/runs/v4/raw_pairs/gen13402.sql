WITH attributed AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM orders src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date < pr.ends_on + INTERVAL 30 DAYS
),
with_windows AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        promo_id,
        promo_code,
        discount_pct,
        MIN(discount) OVER (PARTITION BY store_id) AS store_id_min_discount,
        LAST_VALUE(promo_id, TRUE) OVER (PARTITION BY store_id ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_promo_id,
        RANK() OVER (PARTITION BY channel, status ORDER BY discount) AS discount_rank
    FROM attributed
),
with_windows_orders AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        promo_id,
        promo_code,
        discount_pct,
        store_id_min_discount,
        last_known_promo_id,
        discount_rank,
        CORR(shipping_fee, discount) OVER (PARTITION BY customer_id, status) AS corr_shipping_fee_discount,
        SUM(amount) OVER (PARTITION BY customer_id, channel ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        FIRST_VALUE(channel, TRUE) OVER (ORDER BY discount_rank, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
        SUM(store_id_min_discount) OVER (ORDER BY shipping_fee, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_store_id_min_discount,
        LEAD(shipping_fee, 1) OVER (PARTITION BY customer_id ORDER BY amount, id, promo_id) AS next_shipping_fee
    FROM with_windows
),
enriched AS (
    SELECT
        *,
        SUM(last_known_promo_id) OVER (PARTITION BY customer_id, store_id ORDER BY amount, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last_known_promo_id,
        SUM(discount_rank) OVER (PARTITION BY promo_code) AS promo_code_sum_discount_rank
    FROM with_windows_orders
),
valid AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        promo_id,
        promo_code,
        discount_pct,
        last_known_promo_id,
        discount_rank,
        corr_shipping_fee_discount,
        running_amount,
        first_known_channel,
        cumulative_store_id_min_discount,
        next_shipping_fee,
        running_last_known_promo_id,
        promo_code_sum_discount_rank,
        LEAST(next_shipping_fee, running_last_known_promo_id) AS least_next_shipping_fee_running_last_known_promo_id,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM enriched
    WHERE order_date >= DATE '2023-12-07'
      AND promo_code IS NOT NULL
      AND amount > 870
),
valid_orders AS (
    SELECT
        id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        promo_id,
        promo_code,
        discount_pct,
        last_known_promo_id,
        discount_rank,
        corr_shipping_fee_discount,
        running_amount,
        first_known_channel,
        next_shipping_fee,
        running_last_known_promo_id,
        promo_code_sum_discount_rank,
        least_next_shipping_fee_running_last_known_promo_id,
        channel_clean,
        INITCAP(TRIM(status)) AS status_title,
        COALESCE(channel, 'DIRECT') AS channel_clean_2
    FROM valid
    WHERE order_date > DATE '2024-02-01'
      AND store_id IN (2, 12, 20, 22)
)
SELECT
    CASE WHEN least_next_shipping_fee_running_last_known_promo_id >= 75 THEN 'LARGE' ELSE 'SMALL' END AS least_next_shipping_fee_running_last_known_promo_id_tier,
    CASE WHEN amount >= 1760 THEN 'LARGE' ELSE 'SMALL' END AS amount_tier,
    CASE WHEN running_amount >= 8860 THEN 'LARGE' ELSE 'SMALL' END AS running_amount_tier,
    MIN(shipping_fee) AS min_shipping_fee,
    COUNT(DISTINCT channel) AS distinct_channels,
    SUM(running_amount) FILTER (WHERE channel = 'web') AS web_total_running_amount,
    ROUND(AVG(discount_rank), 2) AS avg_discount_rank,
    ROUND(AVG(corr_shipping_fee_discount), 2) AS avg_corr_shipping_fee_discount,
    ROUND(AVG(corr_shipping_fee_discount), 2) AS avg_corr_shipping_fee_discount_2
FROM valid_orders
GROUP BY 1, 2, 3
ORDER BY avg_corr_shipping_fee_discount_2 DESC
