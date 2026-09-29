WITH cleaned AS (
    SELECT *, LENGTH(carrier) AS carrier_length
    FROM shipments
    WHERE shipped_date >= DATE '2023-03-20'
),
latest AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY cost DESC NULLS LAST, id) = 1
),
scored AS (
    SELECT
        *,
        SUM(cost) OVER (PARTITION BY status ORDER BY carrier_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost,
        SUM(carrier_length) OVER (PARTITION BY order_id, status ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_carrier_length,
        DENSE_RANK() OVER w_running AS carrier_length_dense_rank
    FROM latest
    WHERE carrier_length BETWEEN 35 AND 44
    WINDOW w_running AS (PARTITION BY status, order_id ORDER BY carrier_length)
),
metrics AS (
    SELECT
        *,
        LEAD(cumulative_cost, 1) OVER (ORDER BY delivered_date, id) AS next_cumulative_cost,
        SUM(cost) OVER (ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cost_2,
        AVG(cumulative_carrier_length) OVER (PARTITION BY status, carrier ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cumulative_carrier_length_avg_6
    FROM scored
),
scored_shipments AS (
    SELECT
        id,
        order_id,
        delivered_date,
        status,
        carrier_length,
        cumulative_cost,
        LEAD(cost, 1) OVER (PARTITION BY status ORDER BY next_cumulative_cost, id) AS next_cost,
        NTILE(5) OVER (PARTITION BY carrier ORDER BY cost DESC, id) AS cost_bucket,
        AVG(cumulative_carrier_length) OVER (PARTITION BY status) AS status_avg_cumulative_carrier_length
    FROM metrics
)
SELECT
    id,
    order_id,
    status,
    carrier_length,
    cumulative_cost,
    next_cost,
    cost_bucket,
    status_avg_cumulative_carrier_length,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY delivered_date, id) AS event_seq,
    AVG(status_avg_cumulative_carrier_length) OVER (PARTITION BY status, order_id ORDER BY carrier_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS status_avg_cumulative_carrier_length_avg_6,
    LAST_VALUE(status_avg_cumulative_carrier_length, TRUE) OVER (PARTITION BY order_id ORDER BY next_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_status_avg_cumulative_carrier_length
FROM scored_shipments
