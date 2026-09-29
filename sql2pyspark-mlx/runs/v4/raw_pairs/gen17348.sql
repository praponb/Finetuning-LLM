WITH with_windows AS (
    SELECT
        *,
        SUM(shipping_fee) OVER (PARTITION BY channel) AS channel_sum_shipping_fee,
        SUM(shipping_fee) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_shipping_fee,
        LAG(amount, 2, 0) OVER (PARTITION BY status, channel ORDER BY amount, id) AS prev_amount,
        ROW_NUMBER() OVER (PARTITION BY channel, store_id ORDER BY discount DESC, id) AS seq_no,
        STDDEV_SAMP(amount) OVER (PARTITION BY status, store_id) AS status_stddev_samp_amount
    FROM orders
)
SELECT
    store_id,
    DATE_TRUNC('YEAR', order_date) AS year_start,
    seq_no,
    COUNT(DISTINCT status) AS distinct_status,
    ROUND(SUM(amount) / NULLIF(COUNT(CASE WHEN channel = 'web' THEN 1 END), 0), 2) AS amount_per_web,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count,
    COUNT(*) AS num_rows,
    ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS seq_no_per_store,
    AVG(cumulative_shipping_fee) FILTER (WHERE channel = 'partner') AS partner_avg_cumulative_shipping_fee,
    DENSE_RANK() OVER (PARTITION BY store_id ORDER BY ROUND(SUM(seq_no) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) DESC) AS seq_no_per_store_dense_rank
FROM with_windows
GROUP BY store_id, DATE_TRUNC('YEAR', order_date), seq_no
ORDER BY seq_no DESC NULLS LAST
