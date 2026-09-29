WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        DATE_ADD(shipped_date, 87) AS shipped_date_plus_87d,
        LENGTH(status) AS status_length
    FROM shipments
    WHERE order_id IS NOT NULL
      AND carrier IN ('Kerry', 'FedEx', 'UPS')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(status_length) OVER (PARTITION BY status, order_id ORDER BY shipped_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS status_length_avg_3_prior,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY status_length NULLS LAST) AS status_length_dense_rank
    FROM filtered
),
metrics_shipments AS (
    -- Ranking and gap features
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY status_length_avg_3_prior DESC NULLS FIRST) AS status_length_avg_3_prior_dense_rank,
        RANK() OVER (PARTITION BY order_id ORDER BY cost DESC NULLS LAST) AS cost_rank,
        LAG(status_length_dense_rank, 1, 0) OVER w_running AS prev_status_length_dense_rank,
        DENSE_RANK() OVER (PARTITION BY carrier ORDER BY cost NULLS LAST) AS cost_dense_rank
    FROM metrics
    WINDOW w_running AS (PARTITION BY order_id ORDER BY status_length, id)
),
metrics_shipments_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(cost) OVER (PARTITION BY carrier, status) AS carrier_count_cost,
        ROW_NUMBER() OVER (PARTITION BY carrier ORDER BY prev_status_length_dense_rank, id) AS seq_no,
        AVG(status_length_avg_3_prior_dense_rank) OVER (ORDER BY shipped_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS status_length_avg_3_prior_dense_rank_avg_3,
        SUM(prev_status_length_dense_rank) OVER (PARTITION BY order_id ORDER BY status_length_avg_3_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_status_length_dense_rank,
        MAX(status_length) OVER (PARTITION BY order_id) AS order_id_max_status_length
    FROM metrics_shipments
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, order_id ORDER BY seq_no DESC, id) AS row_num
        FROM metrics_shipments_2
    ) t
    WHERE row_num = 1
)
SELECT
    id,
    carrier,
    shipped_date,
    status,
    status_length,
    status_length_avg_3_prior,
    status_length_dense_rank,
    status_length_avg_3_prior_dense_rank,
    cost_dense_rank,
    CASE WHEN status RLIKE '^(DELIVERED|LOST)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS status_zone
FROM deduped
ORDER BY cost_dense_rank NULLS LAST;
