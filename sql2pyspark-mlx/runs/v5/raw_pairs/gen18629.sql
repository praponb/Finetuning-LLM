WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        LENGTH(status) AS status_length,
        DATE_TRUNC('year', paid_at) AS paid_at_period,
        COALESCE(method, 'UNKNOWN') AS resolved_method
    FROM payments
    WHERE order_id IS NOT NULL
      AND method IS DISTINCT FROM 'paypal'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        order_id,
        status,
        status_length,
        resolved_method,
        CEIL(amount) AS amount_whole,
        CAST(paid_at AS DATE) AS paid_at_day
    FROM filtered
    WHERE order_id IS NOT NULL
),
enriched AS (
    -- Attach orders attributes
    SELECT src.*, o.order_date
    FROM cleaned src
    LEFT JOIN orders o
        ON src.order_id = o.id
),
enriched_payments AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.status_length,
        a.resolved_method,
        a.order_date,
        d.status AS order_status,
        d.order_date AS order_order_date,
        d.channel
    FROM enriched a
    LEFT JOIN orders d
        ON a.order_id = d.id
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, order_status ORDER BY order_date NULLS FIRST, id) AS dedup_rank
    FROM enriched_payments
    QUALIFY dedup_rank = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        status_length,
        resolved_method,
        order_date,
        order_order_date,
        channel,
        dedup_rank
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, order_id ORDER BY status_length DESC, id) = 1
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN status_length >= 63 THEN 'HIGH' ELSE 'LOW' END AS status_length_tier,
        COUNT(*) AS row_count,
        MAX(status_length) FILTER (WHERE channel = 'web') AS web_max_status_length,
        ROUND(SUM(status_length) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS status_length_per_store,
        COUNT(*) AS row_count_2
    FROM latest
    GROUP BY CASE WHEN status_length >= 63 THEN 'HIGH' ELSE 'LOW' END
)
SELECT
    status_length_tier,
    row_count,
    status_length_per_store,
    row_count_2
FROM stats
ORDER BY row_count
