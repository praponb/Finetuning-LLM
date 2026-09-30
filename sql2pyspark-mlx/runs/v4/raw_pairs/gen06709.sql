WITH valid AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        INITCAP(TRIM(channel)) AS channel_title,
        CONCAT_WS(',', channel, status) AS channel_status_key,
        COALESCE(channel, 'NONE') AS channel_filled
    FROM orders
    WHERE order_date >= DATE '2024-01-05'
      AND customer_id IS NOT NULL
      AND channel <> 'web'
),
matched AS (
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM valid s
    LEFT JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.order_date >= pr.starts_on
       AND s.order_date < pr.ends_on + INTERVAL 7 DAYS
),
scored AS (
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY status, promo_code), 4) AS discount_pct_share,
        AVG(promo_id) OVER (ORDER BY amount, id, promo_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS promo_id_avg_6_prior,
        AVG(discount_pct) OVER w_grp AS discount_pct_avg_3,
        ROW_NUMBER() OVER (ORDER BY promo_id, id, promo_id) AS event_seq,
        AVG(promo_id) OVER (PARTITION BY customer_id, status ORDER BY order_date, id, promo_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS promo_id_avg_2_prior
    FROM matched
    WINDOW w_grp AS (PARTITION BY customer_id ORDER BY discount_pct, id, promo_id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY discount_pct_share NULLS FIRST) AS dedup_rank
        FROM scored
    ) x
    WHERE dedup_rank = 1
),
attributed AS (
    SELECT
        a.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM ranked a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.order_date BETWEEN pr.starts_on AND pr.ends_on
),
ranked_orders AS (
    SELECT *
    FROM attributed
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY promo_id_avg_6_prior DESC NULLS LAST, id, promo_id, promo_id_2) = 1
),
base AS (
    SELECT
        *,
        COALESCE(discount_pct_avg_3, 0) AS discount_pct_avg_3_filled,
        INITCAP(TRIM(promo_code)) AS promo_code_title,
        COALESCE(channel, 'N/A') AS channel_clean
    FROM ranked_orders
    WHERE amount IS NOT NULL
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.order_date,
    s.channel_title,
    s.channel_status_key,
    s.promo_id,
    s.discount_pct,
    s.discount_pct_share,
    (SELECT MAX(t2.units) FROM store_sales t2 WHERE t2.store_id = s.store_id) AS max_store_sale_units
FROM base s
ORDER BY discount_pct DESC;
