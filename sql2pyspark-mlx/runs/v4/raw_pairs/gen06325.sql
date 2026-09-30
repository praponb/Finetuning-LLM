WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        TRIM(channel) AS clean_channel,
        COALESCE(channel, 'Unknown') AS channel_filled,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM orders
    WHERE order_date >= DATE '2024-04-01'
      AND customer_id IS NOT NULL
      AND customer_id IN (31, 32)
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM base s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date >= pr.starts_on
       AND s.order_date < pr.ends_on + INTERVAL '30' DAY
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM attributed s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at >= s.order_date
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM not_flagged
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY promo_id DESC NULLS LAST, id, promo_id) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_date,
        status,
        channel,
        shipping_fee,
        clean_channel,
        channel_filled,
        resolved_channel,
        promo_id,
        promo_code,
        discount_pct,
        FLOOR(discount_pct) AS discount_pct_whole,
        COALESCE(promo_code, 'UNKNOWN') AS resolved_promo_code
    FROM latest
    WHERE order_date > DATE '2024-04-06'
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        order_date,
        status,
        channel,
        shipping_fee,
        clean_channel,
        channel_filled,
        resolved_channel,
        promo_id,
        promo_code,
        discount_pct,
        discount_pct_whole,
        resolved_promo_code,
        CUME_DIST() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct_whole NULLS LAST) AS discount_pct_whole_cume_dist,
        MIN(discount_pct_whole) OVER (PARTITION BY channel, promo_code) AS channel_min_discount_pct_whole
    FROM valid
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN promo_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS promo_id_tier,
    COUNT(DISTINCT promo_code) AS distinct_promo_codes,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    SUM(CASE WHEN channel = 'mobile' THEN promo_id ELSE 0 END) AS mobile_promo_id,
    COUNT(DISTINCT status) AS distinct_status
FROM scored
GROUP BY promo_id_tier
HAVING distinct_promo_codes >= 1
