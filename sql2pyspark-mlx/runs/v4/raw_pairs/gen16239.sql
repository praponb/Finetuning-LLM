WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(country) AS country_length,
        SUBSTRING(city, 1, 2) AS city_prefix,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM suppliers
    WHERE LENGTH(supplier_name) >= 13
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        country_length,
        city_prefix,
        city_filled,
        SUM(country_length) OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_country_length,
        RANK() OVER (ORDER BY country_length DESC NULLS FIRST) AS country_length_rank
    FROM filtered
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY country_length_rank DESC NULLS FIRST) AS country_length_rank_percent_rank,
        LAST_VALUE(country_length_rank) IGNORE NULLS OVER (PARTITION BY country ORDER BY country_length_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country_length_rank
    FROM metrics
)
-- Ranking and gap features
SELECT
    *,
    SUM(country_length_rank) OVER (PARTITION BY city ORDER BY country_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_country_length_rank,
    LAG(cumulative_country_length, 1, 0) OVER (PARTITION BY city ORDER BY last_known_country_length_rank, id) AS prev_cumulative_country_length
FROM enriched
