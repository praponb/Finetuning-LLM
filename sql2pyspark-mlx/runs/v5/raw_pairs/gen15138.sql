WITH metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(credit_limit, 1) OVER (PARTITION BY city ORDER BY credit_limit, id) AS next_credit_limit,
        NTILE(4) OVER w_running AS credit_limit_bucket,
        NTILE(3) OVER (PARTITION BY city, country ORDER BY age DESC, id) AS age_bucket
    FROM customers
    WINDOW w_running AS (PARTITION BY country ORDER BY credit_limit, id)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        name,
        email,
        segment,
        signup_date,
        is_active,
        credit_limit_bucket,
        age_bucket
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY next_credit_limit DESC NULLS FIRST, id) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        segment,
        signup_date,
        is_active,
        credit_limit_bucket,
        NTILE(10) OVER (PARTITION BY segment ORDER BY credit_limit_bucket, id) AS credit_limit_bucket_bucket,
        LEAD(age_bucket, 1) OVER (PARTITION BY segment ORDER BY credit_limit_bucket, id) AS next_age_bucket,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY credit_limit_bucket DESC NULLS FIRST) AS credit_limit_bucket_dense_rank,
        MIN(credit_limit_bucket) OVER (PARTITION BY segment) AS segment_min_credit_limit_bucket
    FROM ranked
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.shipping_fee >= 34
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        signup_date,
        is_active,
        credit_limit_bucket_bucket,
        next_age_bucket,
        credit_limit_bucket_dense_rank,
        SUM(credit_limit_bucket) OVER (PARTITION BY segment ORDER BY next_age_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_credit_limit_bucket,
        AVG(segment_min_credit_limit_bucket) OVER (ORDER BY credit_limit_bucket_bucket, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS segment_min_credit_limit_bucket_avg_5_prior,
        FIRST_VALUE(credit_limit_bucket, TRUE) OVER (ORDER BY segment_min_credit_limit_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit_bucket
    FROM not_flagged
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM scored
    EXCEPT ALL
    SELECT id
    FROM customers
    WHERE age > 59
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_credit_limit_bucket >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_credit_limit_bucket_tier,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2,
    COUNT(DISTINCT id) AS distinct_ids,
    GROUPING(CASE WHEN running_credit_limit_bucket >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM kept_rows
GROUP BY ROLLUP (CASE WHEN running_credit_limit_bucket >= 33 THEN 'PREMIUM' ELSE 'STANDARD' END);
