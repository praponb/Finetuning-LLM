WITH valid AS (
    SELECT *, CASE WHEN amount >= 1510 THEN 'High' WHEN amount >= 470 THEN 'Medium' ELSE 'Low' END AS amount_level
    FROM payments
    WHERE amount IS NULL
),
matched_keys AS (
    SELECT order_id, method
    FROM valid
    INTERSECT ALL
    SELECT order_id, method
    FROM payments
    WHERE amount > 1030
),
verified AS (
    SELECT s.*
    FROM valid s
    JOIN matched_keys k ON s.order_id = k.order_id AND s.method = k.method
),
with_windows AS (
    SELECT
        order_id,
        method,
        amount,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY amount DESC NULLS FIRST) AS amount_cume_dist
    FROM verified
    WHERE method <> 'bank_transfer'
),
with_dims AS (
    SELECT
        s.*,
        p.shipping_fee,
        p.discount,
        p.channel
    FROM with_windows s
    LEFT JOIN orders p
        ON s.order_id <=> p.id
),
valid_payments AS (
    SELECT
        method,
        amount_cume_dist,
        discount,
        channel,
        ROUND(amount, 1) AS amount_rounded
    FROM with_dims
    WHERE method IS NOT NULL
      AND method IS NOT DISTINCT FROM 'paypal'
),
enriched AS (
    SELECT
        amount_cume_dist,
        channel,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY discount DESC NULLS FIRST) AS discount_percent_rank,
        AVG(amount_cume_dist) OVER (PARTITION BY channel) AS channel_avg_amount_cume_dist
    FROM valid_payments
),
totals AS (
    SELECT
        channel,
        ROUND(SUM(amount_cume_dist) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS amount_cume_dist_per_store,
        ROUND(SUM(discount_percent_rank) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS discount_percent_rank_per_partner,
        COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count
    FROM enriched
    GROUP BY ROLLUP (channel)
    HAVING ROUND(SUM(amount_cume_dist) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) > 0
)
SELECT amount_cume_dist_per_store, store_count
FROM totals
ORDER BY amount_cume_dist_per_store DESC NULLS FIRST, store_count
