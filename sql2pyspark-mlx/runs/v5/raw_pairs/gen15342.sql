WITH base AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        CONCAT(status, ', ', method) status_method,
        COALESCE(method, 'OTHER') method_clean
    FROM payments
    WHERE status <> 'REFUNDED'
      AND method IS NOT DISTINCT FROM 'bank_transfer'
),
enriched AS (
    SELECT
        s.*,
        o.amount AS order_amount,
        o.channel,
        o.discount
    FROM base s
    LEFT JOIN orders o
        ON s.order_id = o.id
    WHERE o.store_id IS NULL
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY channel ORDER BY amount ASC NULLS FIRST) amount_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY discount DESC, id ASC) seq_no
    FROM enriched
),
metrics AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        status,
        status_method,
        channel,
        discount,
        amount_percent_rank,
        seq_no,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY discount DESC, id ASC) event_seq,
        NTILE(4) OVER (PARTITION BY channel, status ORDER BY seq_no DESC, id ASC) seq_no_bucket
    FROM scored
)
SELECT
    CASE WHEN amount_percent_rank >= 0.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_percent_rank_tier,
    seq_no,
    SORT_ARRAY(COLLECT_SET(method)) AS method_list,
    COUNT(DISTINCT channel) AS distinct_channels,
    COUNT(*) AS event_count,
    RANK() OVER (PARTITION BY CASE WHEN amount_percent_rank >= 0.6 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY COUNT(*) DESC) AS event_count_rank
FROM metrics
GROUP BY CASE WHEN amount_percent_rank >= 0.6 THEN 'PREMIUM' ELSE 'STANDARD' END, seq_no
ORDER BY amount_percent_rank_tier ASC NULLS LAST
