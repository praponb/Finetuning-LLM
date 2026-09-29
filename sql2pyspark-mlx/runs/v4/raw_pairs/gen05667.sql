WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY amount DESC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY amount DESC, id ASC) AS seq_no,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY amount ASC, id ASC) AS amount_bucket
    FROM orders
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, status ORDER BY discount ASC, id ASC) AS row_num
    FROM scored
    QUALIFY row_num = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        status,
        channel,
        amount,
        shipping_fee,
        event_seq,
        seq_no,
        amount_bucket,
        row_num,
        CONCAT_WS('-', status, channel) AS status_channel_key,
        GREATEST(amount_bucket, discount) AS greatest_amount_bucket_discount,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM latest
    WHERE order_date > DATE '2024-04-30'
      AND customer_id IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN greatest_amount_bucket_discount >= 86 THEN 'PREMIUM' ELSE 'STANDARD' END AS greatest_amount_bucket_discount_tier,
    ROUND(AVG(seq_no), 2) AS avg_seq_no,
    SIZE(COLLECT_SET(channel)) AS num_channels,
    COUNT(*) AS row_count,
    ROUND(SUM(event_seq) / NULLIF(COUNT(CASE WHEN status = 'PENDING' THEN 1 END), 0), 2) AS event_seq_per_pending
FROM filtered
GROUP BY CASE WHEN greatest_amount_bucket_discount >= 86 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(*) < 1;
