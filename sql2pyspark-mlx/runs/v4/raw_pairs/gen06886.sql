WITH cleaned AS (
    SELECT
        *,
        MONTH(shipped_date) AS shipped_date_month,
        DATE_ADD(shipped_date, 15) AS shipped_date_plus_15d
    FROM shipments
    WHERE shipped_date BETWEEN '2024-10-19' AND '2025-03-01'
),
latest AS (
    SELECT *
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status ORDER BY delivered_date ASC NULLS FIRST, id ASC) = 1
),
scored AS (
    SELECT
        *,
        NTILE(4) OVER w_running AS cost_bucket,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date_month DESC, id ASC) AS seq_no
    FROM latest
    WINDOW w_running AS (ORDER BY cost DESC, id ASC)
),
summary AS (
    SELECT
        DATE_TRUNC('MONTH', shipped_date) AS activity_month,
        DATE_TRUNC('QUARTER', shipped_date_plus_15d) AS quarter_start,
        CASE WHEN seq_no >= 7 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_tier,
        APPROX_PERCENTILE(seq_no, ARRAY(0.25, 0.5, 0.75)) AS seq_no_quartiles,
        COUNT(*) AS num_rows,
        SUM(CASE WHEN status = 'RETURNED' THEN seq_no ELSE 0 END) AS returned_seq_no,
        COUNT(CASE WHEN status = 'DELIVERED' THEN 1 END) AS delivered_count
    FROM scored
    GROUP BY DATE_TRUNC('MONTH', shipped_date), DATE_TRUNC('QUARTER', shipped_date_plus_15d), CASE WHEN seq_no >= 7 THEN 'LARGE' ELSE 'SMALL' END
    HAVING delivered_count >= 1
)
SELECT
    activity_month,
    quarter_start,
    seq_no_tier,
    seq_no_quartiles,
    num_rows,
    returned_seq_no,
    delivered_count
FROM summary
ORDER BY returned_seq_no DESC
