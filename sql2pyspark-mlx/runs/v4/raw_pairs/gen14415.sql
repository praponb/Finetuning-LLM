WITH filtered AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        cost,
        status,
        LAST_DAY(shipped_date) AS shipped_date_month_end,
        COALESCE(carrier, 'NONE') AS carrier_clean
    FROM shipments
    WHERE delivered_date > TO_DATE('2024-02-22')
),
joined AS (
    SELECT
        src.*,
        b.discount,
        b.channel
    FROM filtered src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY carrier, status ORDER BY cost DESC NULLS LAST, id) AS row_num
        FROM joined
    ) t
    WHERE row_num = 1
),
with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY row_num, id) AS row_seq,
        COUNT(cost) OVER (PARTITION BY carrier) AS carrier_count_cost
    FROM ranked
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(carrier) IGNORE NULLS OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_carrier,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipped_date DESC, id) AS event_seq,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY discount NULLS LAST) AS discount_cume_dist
    FROM with_windows
),
metrics AS (
    SELECT
        *,
        MIN(discount_cume_dist) OVER (PARTITION BY channel, carrier) AS channel_min_discount_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY row_seq, id) AS event_seq_2,
        ROW_NUMBER() OVER (PARTITION BY carrier, channel ORDER BY discount_cume_dist, id) AS seq_no
    FROM enriched
),
grouped AS (
    SELECT
        DATE_TRUNC('MONTH', shipped_date) AS activity_month,
        carrier,
        ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN carrier = 'Flash' THEN 1 END), 0), 2) AS seq_no_per_flash,
        APPROX_PERCENTILE(discount, 0.95) AS discount_p95
    FROM metrics
    GROUP BY DATE_TRUNC('MONTH', shipped_date), carrier
)
SELECT
    activity_month,
    carrier,
    seq_no_per_flash,
    CASE WHEN carrier RLIKE '^(UPS|Flash)$' THEN 'CORE' ELSE 'EXTENDED' END AS carrier_zone
FROM grouped
