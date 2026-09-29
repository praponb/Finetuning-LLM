WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, v
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at) AS days_since_created_at
    FROM flattened
    WHERE key IS NOT DISTINCT FROM 'gift'
      AND customer_id IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        days_since_created_at,
        STDDEV_SAMP(days_since_created_at) OVER w_running AS key_stddev_samp_days_since_created_at,
        MIN(days_since_created_at) OVER w_running AS key_min_days_since_created_at,
        MAX(days_since_created_at) OVER (PARTITION BY customer_id ORDER BY CAST(created_at AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS days_since_created_at_max_30m
    FROM cleaned
    WHERE days_since_created_at BETWEEN 27 AND 38
    WINDOW w_running AS (PARTITION BY key)
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        c.name,
        c.city,
        c.country
    FROM metrics src
    JOIN customers c
        ON src.customer_id = c.id
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.*,
        b.age,
        b.segment,
        b.credit_limit
    FROM enriched s
    LEFT JOIN customers b
        ON s.customer_id = b.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        country,
        CUME_DIST() OVER (ORDER BY age DESC) AS age_cume_dist,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY key_min_days_since_created_at DESC NULLS FIRST) AS key_min_days_since_created_at_dense_rank,
        MIN(days_since_created_at_max_30m) OVER w_hist AS customer_id_min_days_since_created_at_max_30m,
        DENSE_RANK() OVER w_ordered AS key_min_days_since_created_at_dense_rank_2
    FROM joined
    WINDOW w_hist AS (PARTITION BY customer_id, city), w_ordered AS (ORDER BY key_min_days_since_created_at DESC)
),
daily AS (
    -- Grouped metrics
    SELECT
        country,
        SUM(CASE WHEN country = 'UK' THEN age_cume_dist ELSE 0 END) AS uk_age_cume_dist,
        COUNT(DISTINCT country) AS distinct_countries,
        COUNT(*) AS num_rows
    FROM scored
    GROUP BY country
    HAVING SUM(CASE WHEN country = 'UK' THEN age_cume_dist ELSE 0 END) > 0
)
SELECT
    country,
    uk_age_cume_dist,
    num_rows,
    CASE WHEN country RLIKE '^(US|JP)$' THEN 'CORE' ELSE 'EXTENDED' END AS country_zone
FROM daily
ORDER BY uk_age_cume_dist DESC NULLS FIRST;
