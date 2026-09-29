WITH valid AS (
    SELECT *, CAST(shipping_fee AS INT) AS shipping_fee_int
    FROM orders
    WHERE YEAR(order_date) >= 2024
),
metrics AS (
    SELECT
        id,
        channel,
        amount,
        discount,
        shipping_fee,
        LAST_VALUE(channel, TRUE) OVER (PARTITION BY channel, status ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel,
        NTILE(3) OVER (PARTITION BY status, customer_id ORDER BY discount DESC, id ASC) AS discount_bucket,
        FIRST_VALUE(shipping_fee) IGNORE NULLS OVER w_grp AS first_known_shipping_fee,
        SUM(shipping_fee_int) OVER w_recent AS cumulative_shipping_fee_int,
        CORR(shipping_fee, discount) OVER w_recent_2 AS corr_shipping_fee_discount
    FROM valid
    WINDOW w_grp AS (ORDER BY order_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY store_id ORDER BY shipping_fee_int ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent_2 AS (PARTITION BY customer_id, status)
),
not_flagged AS (
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.unit_price >= 360
        )
),
with_windows AS (
    SELECT
        id,
        amount,
        shipping_fee,
        discount_bucket,
        first_known_shipping_fee,
        corr_shipping_fee_discount,
        MAX(cumulative_shipping_fee_int) OVER (PARTITION BY channel ORDER BY corr_shipping_fee_discount ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_max_cumulative_shipping_fee_int,
        AVG(cumulative_shipping_fee_int) OVER (PARTITION BY channel ORDER BY corr_shipping_fee_discount ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_cumulative_shipping_fee_int,
        COUNT(cumulative_shipping_fee_int) OVER (PARTITION BY channel ORDER BY corr_shipping_fee_discount ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_count_cumulative_shipping_fee_int,
        FIRST_VALUE(discount, TRUE) OVER (ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount
    FROM not_flagged
    WHERE shipping_fee < 39
),
with_windows_orders AS (
    SELECT
        id,
        amount,
        discount_bucket,
        corr_shipping_fee_discount,
        last3_max_cumulative_shipping_fee_int,
        last3_count_cumulative_shipping_fee_int,
        AVG(last3_avg_cumulative_shipping_fee_int) OVER w_grp AS running_avg_last3_avg_cumulative_shipping_fee_int,
        MIN(last3_avg_cumulative_shipping_fee_int) OVER w_grp AS running_min_last3_avg_cumulative_shipping_fee_int,
        MAX(last3_avg_cumulative_shipping_fee_int) OVER w_grp AS running_max_last3_avg_cumulative_shipping_fee_int,
        FIRST_VALUE(last3_count_cumulative_shipping_fee_int, TRUE) OVER w_recent AS first_known_last3_count_cumulative_shipping_fee_int
    FROM with_windows
    WINDOW w_grp AS (ORDER BY discount_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (ORDER BY corr_shipping_fee_discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        id,
        amount,
        discount_bucket,
        corr_shipping_fee_discount,
        last3_count_cumulative_shipping_fee_int,
        running_avg_last3_avg_cumulative_shipping_fee_int,
        running_min_last3_avg_cumulative_shipping_fee_int,
        running_max_last3_avg_cumulative_shipping_fee_int,
        last3_max_cumulative_shipping_fee_int + running_min_last3_avg_cumulative_shipping_fee_int AS last3_max_cumulative_shipping_fee_int_plus_running_min_last3_avg_cumulative_shipping_fee_int
    FROM with_windows_orders
    WHERE amount BETWEEN 550 AND 1290
)
SELECT
    id,
    amount,
    discount_bucket,
    corr_shipping_fee_discount,
    last3_count_cumulative_shipping_fee_int,
    running_avg_last3_avg_cumulative_shipping_fee_int,
    running_min_last3_avg_cumulative_shipping_fee_int,
    running_max_last3_avg_cumulative_shipping_fee_int,
    last3_max_cumulative_shipping_fee_int_plus_running_min_last3_avg_cumulative_shipping_fee_int
FROM base
