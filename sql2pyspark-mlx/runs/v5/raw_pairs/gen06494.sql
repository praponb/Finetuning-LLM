WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        paid_at,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share,
        AVG(amount) OVER w_part AS amount_avg_3,
        ROW_NUMBER() OVER w_part_2 AS row_seq,
        SUM(amount) OVER w_part_3 AS method_sum_amount
    FROM payments
    WHERE method IS NOT NULL
    WINDOW w_part AS (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), w_part_2 AS (ORDER BY paid_at, id), w_part_3 AS (PARTITION BY method)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END AS row_seq_tier,
    DATE_TRUNC('QUARTER', paid_at) AS quarter_start,
    COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
    PERCENTILE_APPROX(amount_share, 0.5) AS amount_share_p50,
    APPROX_PERCENTILE(amount_avg_3, 0.9) AS amount_avg_3_p90,
    ROUND(SUM(row_seq) / NULLIF(COUNT(CASE WHEN method = 'paypal' THEN 1 END), 0), 2) AS row_seq_per_paypal,
    AVG(row_seq) AS avg_row_seq
FROM enriched
GROUP BY CASE WHEN row_seq >= 8 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', paid_at);
