WITH filtered AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        TRIM(status) AS clean_status
    FROM shipments
    WHERE status IN ('RETURNED', 'DELIVERED', 'LOST')
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(cost) IGNORE NULLS OVER (ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_cost,
        SUM(cost) OVER w_hist AS running_cost,
        COUNT(cost) OVER (PARTITION BY order_id) AS order_id_count_cost
    FROM filtered
    WINDOW w_hist AS (PARTITION BY order_id, carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    SELECT
        src.id,
        src.order_id,
        src.carrier,
        src.shipped_date,
        src.status,
        o.status AS order_status,
        o.channel,
        o.order_date
    FROM with_windows src
    INNER JOIN orders o
        ON src.order_id = o.id
    WHERE o.channel IN ('web', 'store', 'partner')
),
with_windows_shipments AS (
    SELECT
        id,
        carrier,
        shipped_date,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY order_status, channel ORDER BY order_date DESC, id) AS row_seq,
        ROW_NUMBER() OVER (ORDER BY order_date DESC, id) AS seq_no,
        LAST_VALUE(order_status) IGNORE NULLS OVER (PARTITION BY status ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_order_status
    FROM joined
),
with_windows_shipments_2 AS (
    SELECT
        *,
        COUNT(seq_no) OVER (PARTITION BY carrier ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_seq_no,
        AVG(seq_no) OVER (PARTITION BY carrier ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_seq_no,
        MIN(seq_no) OVER (PARTITION BY carrier ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_seq_no,
        MAX(seq_no) OVER (PARTITION BY carrier ORDER BY seq_no, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_seq_no
    FROM with_windows_shipments
)
SELECT
    last4_count_seq_no,
    CASE WHEN last4_count_seq_no >= 15 THEN 'HIGH' ELSE 'LOW' END AS last4_count_seq_no_tier,
    COUNT(DISTINCT carrier) AS distinct_carriers,
    SUM(last4_max_seq_no) AS total_last4_max_seq_no,
    COUNT(CASE WHEN carrier = 'FedEx' THEN 1 END) AS fedex_count,
    COUNT(*) FILTER (WHERE carrier = 'UPS') AS ups_rows,
    APPROX_PERCENTILE(seq_no, 0.95) AS seq_no_p95,
    DENSE_RANK() OVER (PARTITION BY last4_count_seq_no ORDER BY COUNT(DISTINCT carrier) DESC) AS distinct_carriers_dense_rank
FROM with_windows_shipments_2
GROUP BY last4_count_seq_no, CASE WHEN last4_count_seq_no >= 15 THEN 'HIGH' ELSE 'LOW' END;
