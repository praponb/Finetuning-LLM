WITH with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.method
    FROM orders s
    JOIN LATERAL (
            SELECT c.method
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_latest e
    JOIN promotions pr
        ON COALESCE(e.channel, 'web') = pr.channel
       AND e.order_date >= pr.starts_on
       AND e.order_date <= DATE_ADD(pr.ends_on, 7)
),
scored AS (
    -- Running and moving aggregates
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
        method,
        promo_id,
        promo_code,
        discount_pct,
        NTILE(4) OVER (ORDER BY promo_id, id, promo_id) AS promo_id_bucket,
        RANK() OVER (PARTITION BY method ORDER BY promo_id NULLS FIRST) AS promo_id_rank,
        ROW_NUMBER() OVER (ORDER BY amount DESC, id, promo_id) AS seq_no,
        ROUND(promo_id / SUM(promo_id) OVER (PARTITION BY status, method), 4) AS promo_id_share,
        STDDEV_SAMP(shipping_fee) OVER (PARTITION BY method, status) AS method_stddev_samp_shipping_fee
    FROM attributed
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM scored src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date < pr.ends_on + INTERVAL '30' DAY
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', order_date) AS quarter_start,
        CASE WHEN promo_id_share >= 0.76 THEN 'HIGH' ELSE 'LOW' END AS promo_id_share_tier,
        SIZE(COLLECT_SET(method)) AS num_methods,
        SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
        COUNT(CASE WHEN channel = 'partner' THEN 1 END) AS partner_count
    FROM matched
    GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', order_date), CASE WHEN promo_id_share >= 0.76 THEN 'HIGH' ELSE 'LOW' END), (DATE_TRUNC('QUARTER', order_date)), ())
    HAVING partner_count > 1
)
SELECT
    promo_id_share_tier,
    num_methods,
    partner_count
FROM stats
