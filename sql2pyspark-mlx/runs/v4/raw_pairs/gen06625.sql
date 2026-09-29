WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT id, channel
    FROM orders
    INTERSECT
    SELECT id, channel
    FROM orders
    WHERE channel = 'partner'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM orders s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.channel = k.channel
),
with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM kept_rows e
    INNER JOIN promotions pr
        ON e.channel = pr.channel
       AND e.order_date BETWEEN pr.starts_on AND pr.ends_on
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_window s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.order_id = s.id
              AND x.unit_price IS NOT NULL
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at >= '2025-11-01'
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN discount >= 0.33 THEN 'High' WHEN discount >= 0.09 THEN 'Medium' ELSE 'Low' END AS discount_band,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM eligible
    WHERE channel NOT IN ('store', 'web')
),
wide AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            promo_code,
            channel,
            discount_pct
        FROM valid
    ) src
    PIVOT (
        MAX(discount_pct) AS amount,
        COUNT(1) AS n
        FOR channel IN ('store', 'partner', 'web')
    )
)
SELECT
    promo_code,
    store_amount,
    partner_amount,
    partner_n,
    web_amount,
    web_n
FROM wide
ORDER BY promo_code
