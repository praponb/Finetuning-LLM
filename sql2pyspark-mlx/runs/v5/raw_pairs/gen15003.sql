WITH enriched AS (
    SELECT
        *,
        LAST_VALUE(city, TRUE) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city,
        COUNT(rating) OVER (PARTITION BY country, city) AS country_count_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM suppliers
),
remaining_keys AS (
    SELECT country, city
    FROM enriched
    EXCEPT
    SELECT country, city
    FROM suppliers
    WHERE rating BETWEEN 2.8 AND 3.9
),
reconciled AS (
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.country = k.country AND s.city = k.city
),
enriched_suppliers AS (
    SELECT
        id,
        supplier_name,
        rating,
        country_count_rating,
        running_rating,
        cumulative_rating,
        RANK() OVER (ORDER BY rating DESC) AS rating_rank,
        COUNT(rating) OVER (PARTITION BY city, country) AS city_count_rating
    FROM reconciled
),
matched_keys AS (
    SELECT id
    FROM enriched_suppliers
    INTERSECT
    SELECT id
    FROM suppliers
    WHERE country = 'SG'
),
verified AS (
    SELECT s.*
    FROM enriched_suppliers s
    JOIN matched_keys k ON s.id = k.id
),
metrics AS (
    SELECT
        *,
        RANK() OVER (ORDER BY rating DESC NULLS FIRST) AS rating_rank_2,
        PERCENT_RANK() OVER w_ordered AS country_count_rating_percent_rank,
        RANK() OVER (ORDER BY rating_rank NULLS FIRST) AS rating_rank_rank,
        DENSE_RANK() OVER (ORDER BY cumulative_rating NULLS FIRST) AS cumulative_rating_dense_rank
    FROM verified
    WINDOW w_ordered AS (ORDER BY country_count_rating DESC)
)
SELECT
    CASE WHEN rating_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_rank_tier,
    ROUND(AVG(country_count_rating_percent_rank), 2) AS avg_country_count_rating_percent_rank,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS row_count
FROM metrics
GROUP BY 1
HAVING row_count < 1000;
