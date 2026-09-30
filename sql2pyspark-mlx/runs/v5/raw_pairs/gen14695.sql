WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY shipped_date DESC NULLS LAST, id) AS row_num
    FROM shipments
    QUALIFY row_num = 1
),
kept_keys AS (
    SELECT order_id
    FROM ranked
    EXCEPT
    SELECT order_id
    FROM shipments
    WHERE order_id IN (16, 27)
),
reconciled AS (
    SELECT s.*
    FROM ranked s
    INNER JOIN kept_keys k ON s.order_id = k.order_id
),
joined AS (
    SELECT
        e.id,
        e.order_id,
        e.shipped_date,
        e.delivered_date,
        b.amount,
        b.order_date,
        b.shipping_fee
    FROM reconciled e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
enriched AS (
    SELECT
        *,
        SUM(amount) OVER (PARTITION BY order_id) AS order_id_sum_amount,
        CUME_DIST() OVER (ORDER BY shipping_fee NULLS FIRST) AS shipping_fee_cume_dist,
        PERCENT_RANK() OVER (ORDER BY shipping_fee) AS shipping_fee_percent_rank
    FROM joined
),
matched_keys AS (
    SELECT id
    FROM enriched
    INTERSECT ALL
    SELECT id
    FROM shipments
    WHERE cost < 44
),
kept_rows AS (
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.id = k.id
),
with_dims AS (
    SELECT
        e.*,
        o.channel,
        o.discount
    FROM kept_rows e
    LEFT JOIN orders o
        ON e.order_id <=> o.id
),
scored AS (
    SELECT
        order_id,
        delivered_date,
        amount,
        shipping_fee,
        shipping_fee_cume_dist,
        discount,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY shipping_fee_percent_rank) AS shipping_fee_percent_rank_dense_rank,
        DENSE_RANK() OVER (ORDER BY amount) AS amount_dense_rank
    FROM with_dims
),
summary AS (
    SELECT
        DATE_TRUNC('MONTH', delivered_date) AS activity_month,
        COUNT(DISTINCT order_id) AS distinct_order_ids,
        PERCENTILE_APPROX(amount_dense_rank, 0.5) AS amount_dense_rank_p50,
        COUNT(*) AS num_rows
    FROM scored
    GROUP BY DATE_TRUNC('MONTH', delivered_date)
    HAVING num_rows > 1
)
SELECT
    activity_month,
    amount_dense_rank_p50,
    num_rows
FROM summary
ORDER BY activity_month NULLS LAST;
