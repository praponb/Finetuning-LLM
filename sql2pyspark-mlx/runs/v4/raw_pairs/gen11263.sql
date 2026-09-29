WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        elem.price,
        elem.promo
    FROM (
        SELECT
            id,
            customer_id,
            created_at,
            EXPLODE_OUTER(items) AS elem
        FROM carts
    ) li
),
joined AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.name,
        b.country
    FROM lines a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        country,
        DAYOFWEEK(created_at) AS created_at_dayofweek,
        DATE_FORMAT(created_at, 'yyyy-MM') AS created_at_label
    FROM joined
    WHERE HOUR(created_at) BETWEEN 11 AND 15
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(country) AS clean_country
    FROM cleaned
    WHERE country NOT IN ('US', 'UK')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        country,
        created_at_dayofweek,
        created_at_label,
        clean_country,
        RANK() OVER (PARTITION BY country ORDER BY created_at_dayofweek NULLS FIRST) AS created_at_dayofweek_rank,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY created_at_dayofweek DESC NULLS FIRST) AS created_at_dayofweek_dense_rank
    FROM base
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        country,
        created_at_dayofweek,
        created_at_label,
        clean_country,
        created_at_dayofweek_dense_rank,
        LENGTH(country) AS country_length
    FROM enriched
    WHERE created_at_dayofweek_rank != 5
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN created_at_dayofweek >= 67 THEN 'LARGE' ELSE 'SMALL' END AS created_at_dayofweek_tier,
        CASE WHEN country_length >= 83 THEN 'LARGE' ELSE 'SMALL' END AS country_length_tier,
        PERCENTILE_APPROX(created_at_dayofweek, 0.5) AS created_at_dayofweek_p50,
        COUNT(*) FILTER (WHERE country = 'JP') AS jp_rows
    FROM filtered
    GROUP BY CUBE (CASE WHEN created_at_dayofweek >= 67 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN country_length >= 83 THEN 'LARGE' ELSE 'SMALL' END)
)
SELECT
    created_at_dayofweek_tier,
    country_length_tier,
    created_at_dayofweek_p50,
    jp_rows
FROM totals
