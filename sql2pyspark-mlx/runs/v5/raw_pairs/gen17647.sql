WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.unit_price < 120
        )
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM kept e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.order_date >= pr.starts_on
       AND e.order_date <= DATE_ADD(pr.ends_on, 3)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_date,
        amount,
        discount,
        promo_id,
        promo_code,
        discount_pct,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY promo_id ASC, id ASC, promo_id ASC) AS promo_id_bucket,
        LEAD(amount, 1) OVER (ORDER BY amount ASC, id ASC, promo_id ASC) AS next_amount,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY shipping_fee ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        AVG(amount) OVER (PARTITION BY store_id ORDER BY shipping_fee ASC, id ASC, promo_id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY discount_pct DESC, id ASC, promo_id ASC) AS discount_pct_bucket
    FROM matched
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        promo_id,
        promo_code,
        next_amount,
        last_known_channel,
        amount_avg_6_prior,
        discount_pct_bucket,
        LAG(amount, 2) OVER (PARTITION BY promo_code ORDER BY discount_pct ASC, id ASC, promo_id ASC) AS prev_amount,
        SUM(next_amount) OVER w_grp AS cumulative_next_amount,
        PERCENT_RANK() OVER w_ordered AS discount_pct_bucket_percent_rank,
        RANK() OVER (PARTITION BY promo_code ORDER BY promo_id_bucket ASC NULLS FIRST) AS promo_id_bucket_rank,
        SUM(next_amount) OVER w_grp AS running_next_amount
    FROM scored
    WHERE discount > 0.36
    WINDOW w_grp AS (PARTITION BY promo_code ORDER BY amount_avg_6_prior ASC, id ASC, promo_id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (PARTITION BY promo_code ORDER BY discount_pct_bucket ASC NULLS LAST)
)
-- Aggregate with conditional metrics
SELECT
    promo_id,
    CASE WHEN discount_pct_bucket_percent_rank >= 0.65 THEN 'LARGE' ELSE 'SMALL' END AS discount_pct_bucket_percent_rank_tier,
    SUM(CASE WHEN promo_code = 'BUNDLE' THEN promo_id ELSE 0 END) AS bundle_promo_id,
    SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
    COUNT(*) FILTER (WHERE promo_code = 'BUNDLE') AS bundle_rows,
    ROUND(AVG(discount_pct_bucket_percent_rank), 2) AS avg_discount_pct_bucket_percent_rank,
    APPROX_PERCENTILE(running_next_amount, 0.9) AS running_next_amount_p90,
    PERCENT_RANK() OVER (PARTITION BY promo_id ORDER BY ROUND(AVG(discount_pct_bucket_percent_rank), 2) DESC) AS avg_discount_pct_bucket_percent_rank_percent_rank
FROM with_windows
GROUP BY promo_id, CASE WHEN discount_pct_bucket_percent_rank >= 0.65 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_discount_pct_bucket_percent_rank DESC;
