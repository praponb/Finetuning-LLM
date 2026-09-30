WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(status, 1, 3) AS status_prefix,
        INITCAP(TRIM(method)) AS method_title,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM payments
    WHERE amount < 1560
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.method,
        e.amount,
        e.paid_at,
        e.status,
        e.resolved_method,
        d.channel,
        d.discount,
        d.status AS order_status
    FROM base e
    LEFT JOIN orders d
        ON e.order_id = d.id
    WHERE d.store_id = 9
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        resolved_method,
        channel,
        discount,
        order_status
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_status, order_id ORDER BY paid_at ASC, id ASC) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        FIRST_VALUE(method, TRUE) OVER w_ordered AS first_known_method,
        AVG(discount) OVER (PARTITION BY order_id, order_status ORDER BY paid_at ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_avg_5
    FROM deduped
    WINDOW w_ordered AS (ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
grouped AS (
    -- Grouped metrics
    SELECT
        channel,
        order_status,
        COUNT(DISTINCT method) AS distinct_methods,
        SUM(CASE WHEN method = 'bank_transfer' THEN discount_avg_5 ELSE 0 END) AS bank_transfer_discount_avg_5,
        APPROX_PERCENTILE(discount_avg_5, 0.5) AS discount_avg_5_p50,
        MAX(paid_at) AS last_seen,
        COUNT(*) FILTER (WHERE method = 'bank_transfer') AS bank_transfer_rows,
        GROUPING_ID() AS gid
    FROM scored
    GROUP BY CUBE (channel, order_status)
)
SELECT
    channel,
    order_status,
    bank_transfer_discount_avg_5,
    discount_avg_5_p50,
    last_seen,
    gid
FROM grouped;
