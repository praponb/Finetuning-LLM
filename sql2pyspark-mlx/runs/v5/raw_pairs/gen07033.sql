WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(supplier_name, 1, 3) AS supplier_name_prefix,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM suppliers
    WHERE country = 'SG'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        rating,
        supplier_name_prefix,
        city_clean,
        LENGTH(country) AS country_length
    FROM cleaned
    WHERE city IS NOT DISTINCT FROM 'Chiang Mai'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND UPPER(c.product_name) LIKE '%ESK%'
        )
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, country
    FROM eligible
    EXCEPT ALL
    SELECT id, country
    FROM suppliers
    WHERE country IN ('US', 'DE')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN kept_keys k ON s.id = k.id AND s.country = k.country
),
filtered_suppliers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(supplier_name) AS supplier_name_upper,
        LEAST(rating, country_length) AS least_rating_country_length
    FROM verified
    WHERE country_length <> 44
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(least_rating_country_length) OVER (PARTITION BY country) AS country_min_least_rating_country_length,
        PERCENT_RANK() OVER w_ordered AS country_length_percent_rank
    FROM filtered_suppliers
    WHERE least_rating_country_length >= 40
    WINDOW w_ordered AS (PARTITION BY country ORDER BY country_length NULLS LAST)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        rating,
        supplier_name_prefix,
        country_min_least_rating_country_length,
        country_length_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY country ORDER BY country_length_percent_rank DESC NULLS FIRST) AS country_length_percent_rank_percent_rank,
        RANK() OVER (PARTITION BY country ORDER BY country_length DESC) AS country_length_rank
    FROM scored
)
SELECT
    supplier_name_prefix,
    country_min_least_rating_country_length,
    country_length_percent_rank,
    country_length_percent_rank_percent_rank,
    country_length_rank
FROM metrics
ORDER BY country_length_percent_rank_percent_rank DESC, country_min_least_rating_country_length
