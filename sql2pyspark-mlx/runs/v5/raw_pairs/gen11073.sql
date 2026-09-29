WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_date,
        status,
        channel,
        discount,
        shipping_fee,
        amount + shipping_fee amount_with_shipping,
        shipping_fee - amount amount_with_shipping_2,
        COALESCE(channel, 'NONE') resolved_channel
    FROM orders
    WHERE order_date >= TO_DATE('2023-03-04')
      AND channel IS NOT DISTINCT FROM 'store'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.product_id IS NOT NULL
        )
),
kept_keys AS (
    -- Set intersection on keys
    SELECT channel, status
    FROM eligible
    INTERSECT
    SELECT channel, status
    FROM orders
    WHERE shipping_fee BETWEEN 12 AND 37
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN kept_keys k ON s.channel = k.channel AND s.status = k.status
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM reconciled a
    LEFT JOIN promotions pr
        ON a.channel = pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL '14' DAY
)
-- Window metrics over several different partitions
SELECT
    id,
    status,
    channel,
    discount,
    amount_with_shipping_2,
    promo_code,
    MIN(amount_with_shipping_2) OVER (PARTITION BY status) status_min_amount_with_shipping_2,
    MAX(amount_with_shipping_2) OVER w_grp promo_code_max_amount_with_shipping_2,
    DENSE_RANK() OVER w_part amount_with_shipping_dense_rank,
    PERCENT_RANK() OVER (PARTITION BY channel, status ORDER BY amount_with_shipping_2 DESC NULLS FIRST) amount_with_shipping_2_percent_rank
FROM with_window
WINDOW w_grp AS (PARTITION BY promo_code), w_part AS (PARTITION BY status, channel ORDER BY amount_with_shipping)
