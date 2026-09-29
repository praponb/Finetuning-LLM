WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', country, supplier_name) AS country_supplier_name_key,
        COALESCE(city, 'OTHER') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IS DISTINCT FROM 'Chiang Mai'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(rating AS INT) AS rating_int
    FROM valid
    WHERE city IS DISTINCT FROM 'Tokyo'
      AND city IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND SIZE(ch.tags) >= 1
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        rating,
        country_supplier_name_key,
        city_filled,
        NTILE(4) OVER (PARTITION BY country ORDER BY rating, id) AS rating_bucket,
        SUM(rating) OVER w_running AS cumulative_rating,
        RANK() OVER (PARTITION BY country ORDER BY rating_int DESC) AS rating_int_rank
    FROM kept
    WHERE rating_int > 49
    WINDOW w_running AS (PARTITION BY country ORDER BY rating_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        rating,
        country_supplier_name_key,
        rating_int_rank,
        LEAD(rating_int_rank) OVER w_recent AS next_rating_int_rank,
        LAG(rating_int_rank) OVER w_recent AS prev_rating_int_rank,
        LEAD(rating) OVER w_recent AS next_rating,
        LAG(rating) OVER w_recent AS prev_rating,
        ROW_NUMBER() OVER w_recent AS event_seq,
        STDDEV_SAMP(rating) OVER (PARTITION BY country) AS country_stddev_samp_rating
    FROM scored
    WINDOW w_recent AS (PARTITION BY country ORDER BY rating_bucket, id)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(next_rating_int_rank, rating) AS greatest_next_rating_int_rank_rating
    FROM enriched
    WHERE event_seq IS NOT NULL
),
report AS (
    SELECT
        id,
        rating,
        country_supplier_name_key,
        next_rating_int_rank,
        prev_rating_int_rank,
        next_rating,
        prev_rating,
        country_stddev_samp_rating
    FROM filtered
    ORDER BY prev_rating_int_rank DESC NULLS FIRST
)
SELECT
    country_supplier_name_key,
    next_rating_int_rank,
    next_rating,
    prev_rating,
    country_stddev_samp_rating
FROM report
