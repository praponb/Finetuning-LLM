WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        AVG(cost) OVER (PARTITION BY order_id, carrier ORDER BY shipped_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cost_avg_2,
        FIRST_VALUE(cost) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        LAST_VALUE(carrier) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        ROW_NUMBER() OVER (ORDER BY cost, id) AS row_seq,
        FIRST_VALUE(cost, TRUE) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost_2
    FROM shipments
),
scored_shipments AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        cost_avg_2,
        first_known_cost,
        last_known_carrier,
        row_seq,
        first_known_cost_2,
        STDDEV_SAMP(cost_avg_2) OVER (PARTITION BY order_id) AS order_id_stddev_samp_cost_avg_2,
        SUM(row_seq) OVER (PARTITION BY order_id ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_row_seq,
        NTILE(4) OVER (PARTITION BY order_id ORDER BY first_known_cost_2, id) AS first_known_cost_2_bucket,
        LAST_VALUE(row_seq) IGNORE NULLS OVER (PARTITION BY carrier ORDER BY first_known_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_row_seq,
        FIRST_VALUE(cost_avg_2, TRUE) OVER w_part AS first_known_cost_avg_2
    FROM scored
    WINDOW w_part AS (ORDER BY first_known_cost_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored_shipments_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY order_id ORDER BY row_seq DESC) AS row_seq_dense_rank,
        AVG(last_known_row_seq) OVER (PARTITION BY carrier, order_id ORDER BY first_known_cost_2_bucket, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS last_known_row_seq_avg_2_prior,
        SUM(first_known_cost_2_bucket) OVER (PARTITION BY status ORDER BY first_known_cost_avg_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_known_cost_2_bucket
    FROM scored_shipments
)
SELECT
    id,
    carrier,
    delivered_date,
    status,
    cost_avg_2,
    first_known_cost,
    order_id_stddev_samp_cost_avg_2,
    running_row_seq,
    first_known_cost_2_bucket
FROM scored_shipments_2
ORDER BY order_id_stddev_samp_cost_avg_2 DESC NULLS LAST, delivered_date DESC
