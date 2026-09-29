WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS attr_key, v
),
metrics AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        FIRST_VALUE(attr_key) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY created_at, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_attr_key,
        ROW_NUMBER() OVER (ORDER BY created_at DESC, id, attr_key) AS row_seq
    FROM lines
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY attr_key ORDER BY created_at, id, attr_key) AS rn
        FROM metrics
    ) t
    WHERE rn = 1
),
with_windows AS (
    SELECT
        *,
        FIRST_VALUE(attr_key, TRUE) OVER w_grp AS first_known_attr_key_2,
        SUM(row_seq) OVER (PARTITION BY attr_key ORDER BY row_seq, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq,
        AVG(row_seq) OVER w_running AS row_seq_avg_3
    FROM ranked
    WINDOW w_grp AS (PARTITION BY attr_key ORDER BY created_at, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY attr_key ORDER BY created_at, id, attr_key ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
with_windows_carts AS (
    SELECT
        *,
        MAX(row_seq) OVER w_recent AS running_max_row_seq,
        SUM(row_seq) OVER w_recent AS running_sum_row_seq,
        AVG(row_seq) OVER w_recent AS running_avg_row_seq,
        SUM(row_seq_avg_3) OVER (ORDER BY rn, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq_avg_3
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY attr_key ORDER BY row_seq, id, attr_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    COUNT(*) AS row_count,
    ROUND(SUM(running_avg_row_seq) / NULLIF(COUNT(CASE WHEN attr_key = 'utm_source' THEN 1 END), 0), 2) AS running_avg_row_seq_per_utm_source,
    ROUND(AVG(running_max_row_seq), 2) AS avg_running_max_row_seq,
    SUM(CASE WHEN attr_key = 'weight_kg' THEN cumulative_row_seq ELSE 0 END) AS weight_kg_cumulative_row_seq,
    AVG(row_seq) FILTER (WHERE attr_key = 'weight_kg') AS weight_kg_avg_row_seq,
    COUNT(*) FILTER (WHERE attr_key = 'weight_kg') AS weight_kg_rows
FROM with_windows_carts
GROUP BY DATE_TRUNC('HOUR', created_at);
