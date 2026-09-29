WITH valid AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        CONCAT_WS('-', channel, status) AS channel_status_key
    FROM orders
    WHERE store_id IS NOT NULL
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status ORDER BY order_date) AS rank_in_group
        FROM valid
    ) t
    WHERE rank_in_group = 1
),
attributed AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM latest a
    INNER JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL 30 DAYS
),
with_latest AS (
    SELECT s.*, latest.carrier
    FROM attributed s
    JOIN LATERAL (
            SELECT c.carrier
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    store_id,
    status,
    DATE_TRUNC('MONTH', order_date) AS activity_month,
    AVG(discount_pct) FILTER (WHERE carrier = 'UPS') AS ups_avg_discount_pct,
    APPROX_PERCENTILE(amount, ARRAY(0.25, 0.5, 0.75)) AS amount_quartiles,
    SUM(CASE WHEN channel = 'web' THEN discount ELSE 0 END) AS web_discount,
    APPROX_PERCENTILE(discount, 0.95) AS discount_p95,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY APPROX_PERCENTILE(discount, 0.95) DESC) AS discount_p95_dense_rank
FROM with_latest
GROUP BY store_id, status, DATE_TRUNC('MONTH', order_date)
ORDER BY store_id DESC NULLS LAST
