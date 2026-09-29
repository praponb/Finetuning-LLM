WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY discount NULLS FIRST, id) AS rn
        FROM orders
    ) ranked
    WHERE rn = 1
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.status,
        s.channel,
        s.amount,
        s.discount,
        s.rn,
        top1.shipment_carrier
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.carrier AS shipment_carrier
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(discount, amount) OVER (PARTITION BY shipment_carrier, store_id) AS corr_discount_amount,
        NTILE(5) OVER (ORDER BY amount, id) AS amount_bucket
    FROM latest_match
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.customer_id,
        src.store_id,
        src.status,
        src.amount_bucket,
        s.store_name,
        s.opened_date
    FROM scored src
    LEFT JOIN stores s
        ON src.store_id <=> s.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(amount_bucket) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_amount_bucket,
        AVG(amount_bucket) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_amount_bucket,
        MIN(amount_bucket) OVER (PARTITION BY customer_id ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_amount_bucket
    FROM joined
    WHERE customer_id IN (1, 9, 26, 31)
),
joined_orders AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.store_id,
        e.status,
        e.amount_bucket,
        e.opened_date,
        e.last7_max_amount_bucket,
        e.last7_avg_amount_bucket,
        b.city
    FROM metrics e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN last7_avg_amount_bucket >= 7.6 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_avg_amount_bucket_tier,
        COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count,
        PERCENTILE_APPROX(amount_bucket, 0.9) AS amount_bucket_p90,
        ROUND(SUM(last7_avg_amount_bucket) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS last7_avg_amount_bucket_per_tokyo,
        COUNT_IF(city = 'London') AS london_events,
        SUM(CASE WHEN city = 'Chiang Mai' THEN amount_bucket ELSE 0 END) AS chiang_mai_amount_bucket,
        ROUND(SUM(last7_max_amount_bucket) / NULLIF(COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END), 0), 2) AS last7_max_amount_bucket_per_completed
    FROM joined_orders
    GROUP BY 1
)
SELECT
    last7_avg_amount_bucket_tier,
    london_count,
    amount_bucket_p90,
    last7_avg_amount_bucket_per_tokyo,
    london_events,
    last7_max_amount_bucket_per_completed
FROM stats;
