WITH enriched AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.quantity,
        o.order_date,
        o.amount,
        o.channel
    FROM order_items a
    LEFT JOIN orders o
        ON a.order_id = o.id
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        quantity,
        order_date,
        amount,
        channel,
        SUM(amount) OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM enriched
    WHERE order_date >= '2025-08-01'
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM scored e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.order_date BETWEEN pr.starts_on AND pr.ends_on
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id_2,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM matched s
    LEFT JOIN promotions pr
        ON s.channel = pr.channel
       AND s.order_date BETWEEN pr.starts_on AND pr.ends_on
),
with_window_order_items AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id_3,
        pr.promo_code AS promotion_promo_code_2,
        pr.discount_pct AS promotion_discount_pct_2
    FROM with_window a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL '30' DAY
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        INITCAP(TRIM(channel)) AS channel_title,
        COALESCE(promo_code, 'DIRECT') AS promo_code_clean
    FROM with_window_order_items
    WHERE promo_code IS NOT NULL
),
matched_order_items AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id_4,
        pr.promo_code AS promotion_promo_code_3,
        pr.discount_pct AS promotion_discount_pct_3
    FROM cleaned e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.order_date BETWEEN pr.starts_on AND pr.ends_on
)
-- Window metrics over several different partitions
SELECT
    id,
    order_date,
    promo_id,
    promo_id_2,
    promo_id_3,
    promotion_discount_pct_2,
    channel_title,
    promo_id_4,
    promotion_promo_code_3,
    promotion_discount_pct_3,
    PERCENT_RANK() OVER (PARTITION BY promotion_promo_code ORDER BY promotion_discount_pct) AS promotion_discount_pct_percent_rank,
    SUM(promo_id) OVER (PARTITION BY promotion_promo_code, promotion_promo_code_2 ORDER BY amount, id, promo_id, promo_id_2, promo_id_3, promo_id_4 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_promo_id
FROM matched_order_items
