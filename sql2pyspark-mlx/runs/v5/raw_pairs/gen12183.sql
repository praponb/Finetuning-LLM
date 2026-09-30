WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        amount + shipping_fee AS amount_with_shipping,
        COALESCE(amount, 0) AS amount_filled
    FROM orders
    WHERE status = 'SHIPPED'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        status,
        amount_filled,
        LAG(shipping_fee) OVER w_hist AS prev_shipping_fee,
        LEAD(shipping_fee) OVER w_hist AS next_shipping_fee,
        LEAD(amount_with_shipping) OVER w_hist AS next_amount_with_shipping,
        LAG(amount_with_shipping) OVER w_hist AS prev_amount_with_shipping,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM filtered
    WINDOW w_hist AS (PARTITION BY store_id, channel ORDER BY discount, id)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, amount_filled + next_amount_with_shipping AS amount_filled_plus_next_amount_with_shipping
    FROM with_windows
    WHERE customer_id IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount_filled) OVER (PARTITION BY customer_id ORDER BY prev_shipping_fee, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_filled,
        LEAD(prev_shipping_fee, 1) OVER (ORDER BY next_amount_with_shipping, id) AS next_prev_shipping_fee
    FROM cleaned
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND MONTH(ch.shipped_date) IN (1, 2, 12)
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY cumulative_amount_filled, id) AS latest_rank
    FROM kept
    QUALIFY latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    customer_id,
    CASE WHEN amount_filled >= 83 THEN 'LARGE' ELSE 'SMALL' END AS amount_filled_tier,
    ROUND(AVG(next_prev_shipping_fee), 2) AS avg_next_prev_shipping_fee,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    COUNT_IF(status = 'SHIPPED') AS shipped_events,
    ROUND(SUM(amount_filled) / NULLIF(COUNT(CASE WHEN status = 'SHIPPED' THEN 1 END), 0), 2) AS amount_filled_per_shipped
FROM ranked
GROUP BY customer_id, CASE WHEN amount_filled >= 83 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY amount_filled_per_shipped NULLS FIRST;
