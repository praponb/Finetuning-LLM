WITH scored AS (
    -- Running and moving aggregates
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
        AVG(discount) OVER (PARTITION BY store_id ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS discount_avg_6_prior,
        LAG(discount, 1) OVER (ORDER BY shipping_fee, id) AS prev_discount,
        AVG(amount) OVER (ORDER BY shipping_fee, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        FIRST_VALUE(shipping_fee, TRUE) OVER (PARTITION BY customer_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_shipping_fee,
        RANK() OVER w_grp AS amount_rank
    FROM orders
    WINDOW w_grp AS (PARTITION BY channel ORDER BY amount)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(amount_avg_3_prior, TRUE) OVER w_part AS last_known_amount_avg_3_prior,
        STDDEV_SAMP(shipping_fee) OVER (PARTITION BY store_id) AS store_id_stddev_samp_shipping_fee,
        NTILE(5) OVER w_grp AS discount_avg_6_prior_bucket,
        AVG(amount) OVER (PARTITION BY channel ORDER BY prev_discount, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS amount_avg_5,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM scored
    WINDOW w_part AS (PARTITION BY status ORDER BY amount_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY status ORDER BY discount_avg_6_prior DESC, id), w_hist AS (ORDER BY discount_avg_6_prior, id)
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at <= s.order_date
        )
),
with_dims AS (
    -- Attach customers attributes
    SELECT e.*, c.name
    FROM not_flagged e
    JOIN customers c
        ON e.customer_id = c.id
),
matched_keys AS (
    -- Set intersection on keys
    SELECT status, channel
    FROM with_dims
    INTERSECT
    SELECT status, channel
    FROM orders
    WHERE status IN ('PENDING', 'CANCELLED', 'RETURNED')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims s
    JOIN matched_keys k ON s.status = k.status AND s.channel = k.channel
),
metrics_orders AS (
    -- Ranking and gap features
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
        discount_avg_6_prior,
        prev_discount,
        amount_avg_3_prior,
        first_known_shipping_fee,
        amount_rank,
        last_known_amount_avg_3_prior,
        store_id_stddev_samp_shipping_fee,
        discount_avg_6_prior_bucket,
        amount_avg_5,
        row_seq,
        name,
        MIN(first_known_shipping_fee) OVER (PARTITION BY status, channel) AS status_min_first_known_shipping_fee,
        PERCENT_RANK() OVER w_part AS discount_percent_rank,
        DENSE_RANK() OVER (ORDER BY shipping_fee DESC NULLS LAST) AS shipping_fee_dense_rank
    FROM verified
    WINDOW w_part AS (ORDER BY discount DESC)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics_orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.order_id IN (12, 14, 27, 34)
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        store_id,
        channel,
        amount_avg_3_prior
    FROM eligible
) src
PIVOT (
    SUM(amount_avg_3_prior) AS value_sum
    FOR channel IN ('store' AS store, 'mobile' AS mobile)
);
