WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount,
        discount,
        shipping_fee,
        amount * shipping_fee AS amount_with_shipping,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM orders
    WHERE order_date >= '2024-12-01'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(amount_with_shipping) OVER (PARTITION BY customer_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_with_shipping,
        SUM(shipping_fee) OVER w_ordered AS cumulative_shipping_fee
    FROM valid
    WHERE amount_with_shipping < 75
    WINDOW w_ordered AS (PARTITION BY store_id ORDER BY shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.amount,
        s.discount,
        s.shipping_fee,
        s.amount_with_shipping,
        s.channel_clean,
        s.cumulative_amount_with_shipping,
        top1.unit_price
    FROM metrics s
    JOIN LATERAL (
            SELECT c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(discount) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount,
        LEAD(shipping_fee, 1) OVER (ORDER BY shipping_fee, id) AS next_shipping_fee,
        FIRST_VALUE(unit_price, TRUE) OVER (ORDER BY unit_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_unit_price,
        DENSE_RANK() OVER (ORDER BY cumulative_amount_with_shipping NULLS FIRST) AS cumulative_amount_with_shipping_dense_rank
    FROM latest_match
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at >= '2024-10-01'
        )
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CAST(cumulative_amount_with_shipping_dense_rank AS DOUBLE) AS cumulative_amount_with_shipping_dense_rank_num
    FROM not_flagged
    WHERE shipping_fee BETWEEN 27 AND 31
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN cumulative_amount_with_shipping_dense_rank >= 6 THEN 'LARGE' ELSE 'SMALL' END AS cumulative_amount_with_shipping_dense_rank_tier,
    ROUND(AVG(first_known_unit_price), 2) AS avg_first_known_unit_price,
    ROUND(AVG(shipping_fee), 2) AS avg_shipping_fee,
    COUNT(*) AS event_count
FROM filtered
GROUP BY CASE WHEN cumulative_amount_with_shipping_dense_rank >= 6 THEN 'LARGE' ELSE 'SMALL' END
HAVING ROUND(AVG(shipping_fee), 2) >= 0
