WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('/', status, channel) AS status_channel_key,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM orders
    WHERE store_id IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(channel) IGNORE NULLS OVER w_grp AS last_known_channel,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY amount DESC, id) AS amount_bucket,
        COUNT(shipping_fee) OVER (PARTITION BY channel) AS channel_count_shipping_fee
    FROM base
    WINDOW w_grp AS (PARTITION BY status ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM metrics src
    JOIN promotions pr
        ON src.channel = pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date <= DATE_ADD(pr.ends_on, 3)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY amount_bucket, id, promo_id) AS dedup_rank
    FROM attributed
    QUALIFY dedup_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('QUARTER', order_date) AS quarter_start,
    status,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    CORR(shipping_fee, amount) AS corr_shipping_fee_amount,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) FILTER (WHERE promo_code = 'WELCOME5') AS welcome5_rows,
    GROUPING(DATE_TRUNC('QUARTER', order_date)) AS grouping_level
FROM deduped
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', order_date), status), (DATE_TRUNC('QUARTER', order_date)))
ORDER BY corr_shipping_fee_amount
