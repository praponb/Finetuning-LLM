WITH latest AS (
    SELECT *, RANK() OVER (PARTITION BY customer_id ORDER BY order_date DESC) AS rn
    FROM orders
    QUALIFY rn = 1
),
scored AS (
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY channel ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_amount,
        FIRST_VALUE(discount) IGNORE NULLS OVER w_part AS first_known_discount
    FROM latest
    WINDOW w_part AS (PARTITION BY customer_id, store_id ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        rn,
        running_amount,
        first_known_discount,
        LEAD(discount, 2) OVER (PARTITION BY store_id ORDER BY order_date, id) AS next_discount,
        RANK() OVER w_hist AS shipping_fee_rank,
        NTILE(5) OVER (PARTITION BY store_id, channel ORDER BY running_amount, id) AS running_amount_bucket,
        FIRST_VALUE(first_known_discount, TRUE) OVER (PARTITION BY channel ORDER BY order_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_first_known_discount,
        RANK() OVER (PARTITION BY channel, status ORDER BY running_amount NULLS LAST) AS running_amount_rank
    FROM scored
    WINDOW w_hist AS (PARTITION BY status ORDER BY shipping_fee DESC NULLS LAST)
),
enriched AS (
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        rn,
        running_amount,
        first_known_discount,
        next_discount,
        shipping_fee_rank,
        running_amount_bucket,
        first_known_first_known_discount,
        running_amount_rank,
        LAG(shipping_fee, 1) OVER (ORDER BY shipping_fee_rank, id) AS prev_shipping_fee,
        CUME_DIST() OVER (ORDER BY amount NULLS LAST) AS amount_cume_dist,
        LAST_VALUE(running_amount_bucket) IGNORE NULLS OVER (PARTITION BY store_id ORDER BY running_amount_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_amount_bucket,
        AVG(first_known_discount) OVER (ORDER BY discount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS first_known_discount_avg_3_prior,
        RANK() OVER (PARTITION BY channel, status ORDER BY next_discount DESC) AS next_discount_rank
    FROM metrics
),
kept AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.paid_at < s.order_date
        )
)
SELECT
    DATE_TRUNC('YEAR', order_date) AS year_start,
    ROUND(AVG(first_known_first_known_discount), 2) AS avg_first_known_first_known_discount,
    CORR(discount, amount) AS corr_discount_amount,
    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) AS completed_count
FROM kept
GROUP BY DATE_TRUNC('YEAR', order_date);
