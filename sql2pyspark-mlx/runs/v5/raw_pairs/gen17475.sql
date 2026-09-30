WITH kept_keys AS (
    -- Set difference on keys
    SELECT city
    FROM suppliers
    EXCEPT
    SELECT city
    FROM suppliers
    WHERE country NOT IN ('TH', 'UK')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM suppliers s
    JOIN kept_keys k ON s.city = k.city
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LENGTH(country) AS country_length
    FROM reconciled
    WHERE city != 'New York'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(country, ' - ', city) AS country_city,
        COALESCE(city, 'N/A') AS city_clean
    FROM valid
    WHERE city IS NOT NULL
      AND country IN ('US', 'DE')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        city,
        rating,
        country_city,
        SUM(country_length) OVER w_hist AS country_sum_country_length,
        RANK() OVER (ORDER BY rating NULLS FIRST) AS rating_rank
    FROM filtered
    WHERE country_length < 26
    WINDOW w_hist AS (PARTITION BY country)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT city
    FROM metrics
    INTERSECT
    SELECT city
    FROM suppliers
    WHERE city = 'New York'
),
reconciled_suppliers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN matched_keys k ON s.city = k.city
)
-- Ranking and gap features
SELECT
    *,
    RANK() OVER w_ordered AS rating_rank_rank,
    DENSE_RANK() OVER w_ordered AS rating_rank_dense_rank
FROM reconciled_suppliers
WINDOW w_ordered AS (PARTITION BY city ORDER BY rating_rank DESC NULLS LAST);
