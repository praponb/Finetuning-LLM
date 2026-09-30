WITH enriched AS (
    SELECT
        id,
        carrier,
        shipped_date,
        delivered_date,
        status,
        LEAD(cost) OVER w_part AS next_cost,
        LAG(cost) OVER w_part AS prev_cost,
        ROW_NUMBER() OVER w_part AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY shipped_date, id) AS seq_no_2
    FROM shipments
    WINDOW w_part AS (PARTITION BY carrier ORDER BY shipped_date, id)
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipped_date DESC, id) AS row_num
        FROM enriched
    ) ranked
    WHERE row_num = 1
),
filtered AS (
    SELECT
        *,
        CONCAT_WS(',', carrier, status) AS carrier_status_key,
        COALESCE(carrier, 'DIRECT') AS resolved_carrier
    FROM deduped
    WHERE carrier IS NOT NULL
      AND next_cost <= 26
),
filtered_shipments AS (
    SELECT
        id,
        carrier,
        shipped_date,
        status,
        next_cost,
        prev_cost,
        seq_no,
        seq_no_2,
        row_num,
        carrier_status_key,
        resolved_carrier,
        DATE_TRUNC('WEEK', shipped_date) AS shipped_date_period
    FROM filtered
    WHERE shipped_date >= DATE '2023-03-31'
),
latest AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY carrier ORDER BY shipped_date_period) AS rank_in_group
        FROM filtered_shipments
    ) x
    WHERE rank_in_group = 1
),
enriched_shipments AS (
    SELECT
        id,
        next_cost,
        prev_cost,
        seq_no,
        carrier_status_key,
        rank_in_group,
        NTILE(3) OVER (ORDER BY seq_no_2, id) AS seq_no_2_bucket,
        SUM(seq_no) OVER (ORDER BY seq_no_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_seq_no
    FROM latest
    WHERE seq_no IS NOT NULL
)
SELECT
    CASE WHEN next_cost >= 103 THEN 'LARGE' ELSE 'SMALL' END AS next_cost_tier,
    seq_no,
    PERCENTILE_APPROX(seq_no_2_bucket, 0.95) AS seq_no_2_bucket_p95,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    COUNT(*) AS event_count,
    COUNT(*) AS cnt,
    APPROX_PERCENTILE(seq_no_2_bucket, ARRAY(0.25, 0.5, 0.75)) AS seq_no_2_bucket_quartiles
FROM enriched_shipments
GROUP BY CASE WHEN next_cost >= 103 THEN 'LARGE' ELSE 'SMALL' END, seq_no
HAVING seq_no_2_bucket_p95 > 0
ORDER BY event_count DESC;
