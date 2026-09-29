WITH base AS (
    -- Basic cleansing and defaults
    SELECT *, ADD_MONTHS(delivered_date, 3) AS delivered_date_plus_months
    FROM shipments
    WHERE delivered_date >= DATE '2023-07-26'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC NULLS LAST, id) = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        s.*,
        d.amount,
        d.status AS order_status,
        d.channel
    FROM deduped s
    LEFT JOIN orders d
        ON s.order_id <=> d.id
    WHERE d.discount > 0.13
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_dims src
    LEFT JOIN promotions pr
        ON COALESCE(src.channel, 'web') = pr.channel
       AND src.delivered_date >= pr.starts_on
       AND src.delivered_date < pr.ends_on + INTERVAL 30 DAYS
),
by_type AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            order_status,
            promo_code,
            discount_pct
        FROM with_window
    ) src
    PIVOT (
        MAX(discount_pct) AS total,
        COUNT(1) AS cnt
        FOR promo_code IN ('VIP' AS vip, 'BUNDLE' AS bundle)
    )
)
SELECT
    order_status,
    bundle_total,
    bundle_cnt,
    CASE WHEN order_status RLIKE '^(COMPLETED|SHIPPED)$' THEN 'PRIORITY' ELSE 'CORE' END AS order_status_zone
FROM by_type
ORDER BY order_status DESC NULLS LAST, bundle_cnt DESC
