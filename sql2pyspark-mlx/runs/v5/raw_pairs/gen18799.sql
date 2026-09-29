WITH enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id, status ORDER BY amount DESC, id ASC) AS seq_no,
        FIRST_VALUE(discount, TRUE) OVER (PARTITION BY status ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount,
        STDDEV_SAMP(discount) OVER (PARTITION BY status) AS status_stddev_samp_discount
    FROM orders
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY order_date DESC, id ASC) AS event_seq,
        RANK() OVER (ORDER BY amount ASC) AS amount_rank
    FROM enriched
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.credit_limit,
        c.age
    FROM with_windows src
    LEFT JOIN customers c
        ON src.customer_id = c.id
    WHERE c.signup_date > '2023-03-23'
),
with_windows_orders AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(age) OVER w_part AS running_max_age,
        MIN(age) OVER w_part AS running_min_age,
        COUNT(age) OVER w_part AS running_count_age
    FROM with_dims
    WINDOW w_part AS (PARTITION BY channel ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN first_known_discount >= 0.37 THEN 'HIGH' ELSE 'LOW' END AS first_known_discount_tier,
        status,
        CASE WHEN discount >= 0.32 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_tier,
        COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count,
        APPROX_PERCENTILE(running_min_age, 0.9) AS running_min_age_p90,
        MAX(running_max_age) FILTER (WHERE channel = 'store') AS store_max_running_max_age,
        COUNT(DISTINCT id) AS distinct_ids
    FROM with_windows_orders
    GROUP BY 1, 2, 3
)
SELECT
    first_known_discount_tier,
    status,
    discount_tier,
    store_count,
    store_max_running_max_age,
    distinct_ids,
    CASE WHEN status RLIKE '^(CANCELLED|SHIPPED)$' THEN 'OTHER_ZONE' WHEN status RLIKE '^(COMPLETED|PENDING)$' THEN 'GROUP_B' ELSE 'CORE' END AS status_zone
FROM stats
ORDER BY status ASC NULLS LAST;
