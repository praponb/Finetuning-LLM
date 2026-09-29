WITH with_windows AS (
    SELECT
        *,
        RANK() OVER (ORDER BY amount NULLS FIRST) AS amount_rank,
        DENSE_RANK() OVER w_part AS amount_dense_rank
    FROM orders
    WINDOW w_part AS (ORDER BY amount DESC NULLS FIRST)
),
with_dims AS (
    SELECT
        e.*,
        d.signup_date,
        d.city,
        d.segment
    FROM with_windows e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_percent_rank,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY shipping_fee DESC) AS shipping_fee_cume_dist,
        AVG(amount_dense_rank) OVER (ORDER BY signup_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS amount_dense_rank_avg_2,
        LAG(discount, 2) OVER (PARTITION BY segment ORDER BY shipping_fee, id) AS prev_discount,
        NTILE(5) OVER (ORDER BY amount, id) AS amount_bucket
    FROM with_dims
)
SELECT *
FROM (
    SELECT
        id,
        channel,
        shipping_fee,
        prev_discount
    FROM scored
) src
UNPIVOT (
    value FOR metric_name IN (shipping_fee, prev_discount)
);
