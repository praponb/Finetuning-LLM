WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, INITCAP(TRIM(status)) AS status_title
    FROM orders
    WHERE order_date >= TO_DATE('2023-07-25')
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM valid a
    INNER JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.order_date BETWEEN pr.starts_on AND pr.ends_on
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM attributed src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.order_date >= pr.starts_on
       AND src.order_date < pr.ends_on + INTERVAL 14 DAYS
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_window s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.order_id = 22
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(discount_pct AS INT) AS discount_pct_int,
        ROUND(discount / promo_id_2, 2) AS discount_per_promo_id_2,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM not_flagged
    WHERE store_id IS NOT NULL
      AND promotion_promo_code IS DISTINCT FROM 'WELCOME5'
)
SELECT
    id,
    customer_id,
    store_id,
    status,
    channel,
    amount,
    promo_id,
    promo_code,
    discount_pct,
    CASE WHEN status RLIKE '^(COMPLETED|PENDING)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS status_zone
FROM cleaned
ORDER BY promo_id NULLS LAST, store_id DESC;
