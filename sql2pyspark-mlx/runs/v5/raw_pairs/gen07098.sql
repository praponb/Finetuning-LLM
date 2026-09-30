WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        CONCAT_WS('-', city, country) AS city_country_key,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY country) AS country_avg_rating,
        FIRST_VALUE(city) IGNORE NULLS OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_city,
        RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS rating_rank,
        ROW_NUMBER() OVER (ORDER BY rating, id) AS seq_no,
        COUNT(rating) OVER (PARTITION BY country) AS country_count_rating
    FROM cleaned
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        supplier_name,
        country,
        city,
        first_known_city,
        rating_rank,
        seq_no,
        country_count_rating
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY country_avg_rating, id) AS latest_rank
        FROM metrics
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(country_count_rating) OVER (PARTITION BY city, country ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_country_count_rating,
        AVG(seq_no) OVER (PARTITION BY city, country ORDER BY rating_rank, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS seq_no_avg_6,
        NTILE(5) OVER (PARTITION BY country ORDER BY seq_no DESC, id) AS seq_no_bucket
    FROM latest
    WHERE city IN ('London', 'Singapore', 'Berlin')
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        country,
        first_known_city,
        rating_rank,
        seq_no
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY cumulative_country_count_rating DESC, id) AS rn
        FROM enriched
    ) t
    WHERE rn = 1
)
SELECT
    id,
    country,
    rating_rank,
    seq_no
FROM deduped
ORDER BY country NULLS FIRST;
