WITH with_dims AS (
    SELECT
        a.id,
        a.order_id,
        a.carrier,
        a.delivered_date,
        a.cost,
        a.status,
        b.status AS order_status,
        b.amount,
        b.channel
    FROM shipments a
    INNER JOIN orders b
        ON a.order_id <=> b.id
),
attributed AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_dims src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.delivered_date BETWEEN pr.starts_on AND pr.ends_on
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY discount_pct ASC NULLS LAST) AS discount_pct_cume_dist,
        LAG(cost, 1) OVER (ORDER BY amount ASC, id ASC, promo_id ASC) AS prev_cost,
        NTILE(3) OVER (PARTITION BY order_status ORDER BY discount_pct DESC, id ASC, promo_id ASC) AS discount_pct_bucket
    FROM attributed
),
with_windows AS (
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY status ORDER BY promo_id DESC, id ASC, promo_id ASC) AS promo_id_bucket,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM metrics
    WINDOW w_running AS (PARTITION BY order_id ORDER BY discount_pct_cume_dist ASC, id ASC, promo_id ASC)
)
SELECT
    id,
    carrier,
    delivered_date,
    cost,
    order_status,
    amount,
    channel,
    promo_id,
    promo_code,
    CASE WHEN order_status RLIKE '^(SHIPPED|COMPLETED)$' THEN 'CORE' WHEN order_status RLIKE '^(PENDING|CANCELLED)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS order_status_zone
FROM with_windows
ORDER BY cost DESC;
