WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM orders src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.order_date BETWEEN pr.starts_on AND pr.ends_on
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        promo_id,
        promo_code,
        CORR(amount, discount) OVER w_recent AS corr_amount_discount,
        LAST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY discount, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_shipping_fee,
        SUM(amount) OVER w_recent_2 AS cumulative_amount,
        DENSE_RANK() OVER (ORDER BY discount_pct) AS discount_pct_dense_rank,
        AVG(shipping_fee) OVER (PARTITION BY store_id, customer_id ORDER BY discount, id, promo_id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS shipping_fee_avg_6_prior
    FROM matched
    WHERE discount_pct IS NOT NULL
    WINDOW w_recent AS (PARTITION BY customer_id, promo_code), w_recent_2 AS (PARTITION BY store_id ORDER BY discount_pct, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        promo_id,
        promo_code,
        last_known_shipping_fee,
        discount_pct_dense_rank,
        shipping_fee_avg_6_prior,
        MAX(discount) OVER w_running AS running_max_discount,
        AVG(discount) OVER w_running AS running_avg_discount,
        SUM(discount) OVER w_running AS running_sum_discount
    FROM with_windows
    WINDOW w_running AS (PARTITION BY store_id ORDER BY last_known_shipping_fee, id, promo_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(channel) AS clean_channel,
        GREATEST(promo_id, last_known_shipping_fee) AS greatest_promo_id_last_known_shipping_fee,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM enriched
    WHERE channel IS NOT NULL
      AND discount_pct_dense_rank < 7
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date <= s.order_date
        )
),
enriched_orders AS (
    -- Ranking and gap features
    SELECT
        id,
        order_date,
        discount,
        promo_id,
        last_known_shipping_fee,
        discount_pct_dense_rank,
        clean_channel,
        greatest_promo_id_last_known_shipping_fee,
        resolved_channel,
        LEAD(last_known_shipping_fee) OVER w_grp AS next_last_known_shipping_fee,
        LAG(last_known_shipping_fee) OVER w_grp AS prev_last_known_shipping_fee,
        LAG(shipping_fee_avg_6_prior) OVER w_grp AS prev_shipping_fee_avg_6_prior,
        LEAD(shipping_fee_avg_6_prior) OVER w_grp AS next_shipping_fee_avg_6_prior,
        ROW_NUMBER() OVER w_grp AS event_seq
    FROM not_flagged
    WINDOW w_grp AS (PARTITION BY promo_code, status ORDER BY discount_pct_dense_rank, id, promo_id)
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('YEAR', order_date) AS year_start,
    APPROX_PERCENTILE(last_known_shipping_fee, 0.5) AS last_known_shipping_fee_p50,
    MAX(greatest_promo_id_last_known_shipping_fee) AS max_greatest_promo_id_last_known_shipping_fee,
    PERCENTILE_APPROX(discount_pct_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_dense_rank_quartiles,
    PERCENTILE_APPROX(event_seq, 0.5) AS event_seq_p50,
    PERCENTILE_APPROX(greatest_promo_id_last_known_shipping_fee, ARRAY(0.25, 0.5, 0.75)) AS greatest_promo_id_last_known_shipping_fee_quartiles
FROM enriched_orders
GROUP BY DATE_TRUNC('YEAR', order_date)
HAVING PERCENTILE_APPROX(event_seq, 0.5) > 0
ORDER BY last_known_shipping_fee_p50 DESC;
