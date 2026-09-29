WITH remaining_keys AS (
    SELECT status, id
    FROM payments
    INTERSECT ALL
    SELECT status, id
    FROM payments
    WHERE order_id IN (5, 17, 20, 27)
),
reconciled AS (
    SELECT s.*
    FROM payments s
    JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
),
enriched AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        AVG(amount) OVER (PARTITION BY method ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_avg_7d,
        RANK() OVER (PARTITION BY order_id ORDER BY amount NULLS LAST) AS amount_rank,
        RANK() OVER (PARTITION BY status ORDER BY amount DESC) AS amount_rank_2,
        ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) AS amount_share,
        ROUND(amount / SUM(amount) OVER (PARTITION BY method), 4) AS amount_share_2
    FROM reconciled
),
enriched_payments AS (
    SELECT
        src.id,
        src.order_id,
        src.amount_share,
        p.channel
    FROM enriched src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
scored AS (
    SELECT
        order_id,
        amount_share,
        channel,
        RANK() OVER (ORDER BY amount_share) AS amount_share_rank,
        RANK() OVER (PARTITION BY channel ORDER BY amount_share NULLS LAST) AS amount_share_rank_2,
        MAX(amount_share) OVER w_part AS channel_max_amount_share,
        STDDEV_SAMP(amount_share) OVER w_part AS channel_stddev_samp_amount_share
    FROM enriched_payments
    WHERE channel <> 'partner'
    WINDOW w_part AS (PARTITION BY channel)
),
summary AS (
    SELECT
        CASE WHEN amount_share_rank_2 >= 6 THEN 'LARGE' ELSE 'SMALL' END AS amount_share_rank_2_tier,
        order_id,
        CASE WHEN amount_share_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS amount_share_rank_tier,
        SUM(amount_share_rank_2) FILTER (WHERE channel = 'mobile') AS mobile_total_amount_share_rank_2,
        COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
        SUM(channel_stddev_samp_amount_share) AS total_channel_stddev_samp_amount_share,
        APPROX_PERCENTILE(amount_share, ARRAY(0.25, 0.5, 0.75)) AS amount_share_quartiles
    FROM scored
    GROUP BY CASE WHEN amount_share_rank_2 >= 6 THEN 'LARGE' ELSE 'SMALL' END, order_id, CASE WHEN amount_share_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    s.amount_share_rank_2_tier,
    s.order_id,
    s.amount_share_rank_tier,
    s.mobile_total_amount_share_rank_2,
    s.store_count,
    s.amount_share_quartiles,
    (SELECT AVG(o2.quantity) FROM order_items o2 WHERE o2.order_id = s.order_id) AS avg_order_item_quantity
FROM summary s
ORDER BY avg_order_item_quantity NULLS LAST;
