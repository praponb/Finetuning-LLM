WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM orders src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'store') = pr.channel
       AND src.order_date BETWEEN pr.starts_on AND pr.ends_on
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(promo_id) OVER w_hist AS last3_min_promo_id,
        MAX(promo_id) OVER w_hist AS last3_max_promo_id,
        AVG(promo_id) OVER w_hist AS last3_avg_promo_id,
        COUNT(promo_id) OVER w_hist AS last3_count_promo_id
    FROM matched
    WINDOW w_hist AS (PARTITION BY store_id ORDER BY shipping_fee ASC, id ASC, promo_id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct ASC, id ASC, promo_id ASC) AS rank_in_group
    FROM metrics
    QUALIFY rank_in_group = 1
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM ranked src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date <= DATE_ADD(pr.ends_on, 3)
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.status,
        s.amount,
        s.discount,
        s.shipping_fee,
        s.promo_id,
        s.discount_pct,
        s.last3_min_promo_id,
        s.last3_max_promo_id,
        s.last3_avg_promo_id,
        s.rank_in_group,
        s.promo_id_2,
        s.promotion_promo_code,
        top1.quantity
    FROM with_window s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.status,
    s.amount,
    s.shipping_fee,
    s.promo_id,
    s.discount_pct,
    s.last3_max_promo_id,
    CASE WHEN status RLIKE '^(PENDING|COMPLETED)$' THEN 'PRIORITY' ELSE 'GROUP_A' END AS status_zone,
    (SELECT AVG(x.revenue) FROM store_sales x WHERE x.store_id = s.store_id) AS avg_store_sale_revenue
FROM latest_match s
ORDER BY amount DESC;
