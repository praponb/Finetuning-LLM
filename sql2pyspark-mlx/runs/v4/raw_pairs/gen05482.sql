WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) e AS tag
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CAST(created_at AS DATE) AS created_at_day
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-01 21:00:00'
      AND tag IS NOT NULL
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, customer_id
    FROM cleaned
    INTERSECT ALL
    SELECT id, customer_id
    FROM carts
    WHERE HOUR(created_at) BETWEEN 12 AND 15
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.segment,
        p.country,
        p.email
    FROM verified a
    LEFT JOIN customers p
        ON a.customer_id = p.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        created_at,
        tag,
        segment,
        country,
        email,
        CONCAT_WS('-', segment, country) AS segment_country_key,
        CASE WHEN country = 'JP' THEN 1 ELSE 0 END AS is_jp
    FROM enriched
    WHERE customer_id IS NOT NULL
      AND email LIKE '%@company.co'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM base
    INTERSECT ALL
    SELECT customer_id
    FROM carts
    WHERE customer_id = 31
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN kept_keys k ON s.customer_id = k.customer_id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(is_jp) OVER (PARTITION BY customer_id) AS customer_id_stddev_samp_is_jp,
        SUM(is_jp) OVER (PARTITION BY segment ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS is_jp_sum_30m,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY is_jp DESC) AS is_jp_percent_rank
    FROM reconciled
)
SELECT
    customer_id,
    created_at,
    tag,
    segment,
    email,
    is_jp,
    customer_id_stddev_samp_is_jp,
    is_jp_sum_30m,
    is_jp_percent_rank
FROM metrics;
