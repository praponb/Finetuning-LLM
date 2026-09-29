WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, v
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.key,
        p.credit_limit,
        p.city
    FROM exploded s
    INNER JOIN customers p
        ON s.customer_id <=> p.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(credit_limit) AS credit_limit_whole,
        COALESCE(key, 'UNKNOWN') AS key_filled
    FROM joined
    WHERE credit_limit <= 7300
),
enriched AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.key,
        e.city,
        e.credit_limit_whole,
        b.country
    FROM base e
    LEFT JOIN customers b
        ON e.customer_id = b.id
    WHERE b.is_active
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        FLOOR(credit_limit_whole) AS credit_limit_whole_whole,
        CONCAT_WS('-', country, key) AS country_key_key
    FROM enriched
    WHERE customer_id IS NOT NULL
      AND credit_limit_whole BETWEEN 59 AND 76
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        customer_id,
        country,
        credit_limit_whole_whole,
        PERCENT_RANK() OVER (ORDER BY credit_limit_whole NULLS LAST) AS credit_limit_whole_percent_rank,
        RANK() OVER (ORDER BY credit_limit_whole DESC NULLS FIRST) AS credit_limit_whole_rank,
        RANK() OVER (PARTITION BY city ORDER BY credit_limit_whole NULLS FIRST) AS credit_limit_whole_rank_2
    FROM cleaned
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, credit_limit_whole_rank * credit_limit_whole_percent_rank AS credit_limit_whole_rank_times_credit_limit_whole_percent_rank
    FROM metrics
    WHERE credit_limit_whole_rank BETWEEN 4 AND 8
),
grouped AS (
    -- Grouped metrics
    SELECT
        customer_id,
        COUNT(DISTINCT customer_id) AS distinct_customer_ids,
        COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
        ROUND(SUM(credit_limit_whole_percent_rank) / NULLIF(COUNT(CASE WHEN country = 'UK' THEN 1 END), 0), 2) AS credit_limit_whole_percent_rank_per_uk,
        ROUND(SUM(credit_limit_whole_percent_rank) / NULLIF(COUNT(CASE WHEN country = 'UK' THEN 1 END), 0), 2) AS credit_limit_whole_percent_rank_per_uk_2,
        GROUPING(customer_id) AS grouping_level
    FROM valid
    GROUP BY ROLLUP (customer_id)
)
SELECT sg_count, credit_limit_whole_percent_rank_per_uk_2
FROM grouped
ORDER BY credit_limit_whole_percent_rank_per_uk_2 NULLS FIRST;
